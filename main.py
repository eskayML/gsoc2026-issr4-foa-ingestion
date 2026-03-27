import os
import json
import csv
import argparse
import requests
import re
from datetime import datetime
from typing import List, Dict, Optional
from pydantic import BaseModel, Field
from bs4 import BeautifulSoup
import trafilatura
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()

# --- Pydantic Schema Definition ---
class FOAMetadata(BaseModel):
    generated_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat() + "Z")
    schema_version: str = "1.8.0"
    extractor_engine: str = "ISSR4-Context-Aware-Engine"

class FOARecord(BaseModel):
    foa_id: str
    title: str
    agency: str
    open_date: Optional[str]
    close_date: Optional[str]
    eligibility: str
    program_description: str
    award_range: str
    source_url: str
    tags: Dict[str, List[str]]
    tag_scores: Dict[str, float]

class FOAResponse(BaseModel):
    metadata: FOAMetadata = Field(default_factory=FOAMetadata)
    data: FOARecord

# --- Extraction Engine ---
class ExtractionEngine:
    def __init__(self, url: str):
        self.url = url
        self.raw_html = ""
        self.clean_text = ""
        self.soup = None

    def fetch(self):
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'}
        try:
            response = requests.get(self.url, headers=headers, timeout=15)
            response.raise_for_status()
            self.raw_html = response.text
        except Exception as e:
            console.print(f"[bold red]❌ Network Error: {self.url} ({e})[/bold red]")
            raise
        
        self.soup = BeautifulSoup(self.raw_html, 'html.parser')
        extracted = trafilatura.extract(self.raw_html, include_tables=True, include_links=False)
        self.clean_text = extracted if extracted else self.soup.get_text(separator='\n', strip=True)

    def sanitize_string(self, text: str) -> str:
        """
        Removes characters that break JSON/CSV: control characters, 
        excessive backslashes, and normalizes whitespaces.
        """
        if not text: return ""
        # Remove control characters (non-printable)
        text = "".join(ch for ch in text if ch.isprintable() or ch in "\n\t")
        # Normalize whitespace and remove common breaking escape chars
        text = text.replace('\\', '/').replace('"', "'")
        return " ".join(text.split())

    def extract_award_range(self) -> str:
        text_lower = self.clean_text.lower()
        search_areas = ["award information", "anticipated funding", "award size"]
        extracted_nums = []
        for anchor in search_areas:
            idx = text_lower.find(anchor)
            if idx != -1:
                window = self.clean_text[idx:idx+2500]
                matches = re.findall(r'\$\s*([\d,]+)', window)
                for m in matches:
                    try:
                        val = int(re.sub(r'[,.]', '', m))
                        if val > 1000: extracted_nums.append(val)
                    except: pass
        if extracted_nums:
            unique_vals = sorted(list(set(extracted_nums)))
            if len(unique_vals) >= 2:
                return f"${unique_vals[0]:,} - ${unique_vals[-1]:,}"
            return f"Up to ${unique_vals[0]:,}"
        return "Not specified"

    def extract_fields(self) -> dict:
        title = self.soup.find('title').text.strip() if self.soup.find('title') else "FOA Document"
        foa_id_match = re.search(r'([A-Z]+[\s-]*\d{2}-\d{3})', self.clean_text)
        foa_id = foa_id_match.group(0).replace(' ', '') if foa_id_match else f"FOA-INGEST-{int(datetime.now().timestamp())}"
        
        agency = "US Government Agency"
        if "nsf.gov" in self.url.lower(): agency = "National Science Foundation (NSF)"
        elif "grants.gov" in self.url.lower(): agency = "Grants.gov / Federal"

        date_matches = re.findall(r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}', self.clean_text)
        
        def to_iso(d_str):
            if not d_str: return None
            try: return datetime.strptime(d_str, "%B %d, %Y").strftime("%Y-%m-%d")
            except: return None

        # Eligibility Section Capture
        eligibility = "Refer to source URL."
        elig_match = re.search(r'(?i)Who May Submit Proposals:(.*?)(?=Who May Serve as PI|V\. Proposal|$)', self.clean_text, re.DOTALL)
        if elig_match: eligibility = elig_match.group(1).strip()
        
        # Synopsis Capture
        description = self.clean_text
        desc_match = re.search(r'(?i)(Synopsis of Program|Program Description)[:\s]+(.*?)(?=\nIII\.|\nAward Information|$)', self.clean_text, re.DOTALL)
        if desc_match: description = desc_match.group(2).strip()

        return {
            "foa_id": foa_id,
            "title": self.sanitize_string(title),
            "agency": agency,
            "open_date": to_iso(date_matches[0]) if len(date_matches) > 0 else None,
            "close_date": to_iso(date_matches[-1]) if len(date_matches) > 1 else None,
            "eligibility": self.sanitize_string(eligibility)[:2000],
            "program_description": self.sanitize_string(description),
            "award_range": self.extract_award_range(),
            "source_url": self.url,
        }

# --- Semantic Tagger ---
class SemanticTagger:
    def __init__(self, ontology_path="ontology.json"):
        with open(ontology_path, 'r') as f:
            self.ontology = json.load(f)

    def group_tags(self, text: str) -> Dict[str, List[str]]:
        text_lower = text.lower()
        grouped = {cat: [] for cat in self.ontology.keys()}
        raw_scores = {}
        for category, tags in self.ontology.items():
            for tag_name, tag_data in tags.items():
                hits = sum(1 for kw in tag_data['keywords'] if kw in text_lower)
                if hits > 0:
                    grouped[category].append(tag_name)
                    raw_scores[tag_name] = (hits * 0.3) * tag_data['weight']
        
        # --- Normalizing Scores to add up to 1.0 (Probabilistic Distribution) ---
        total_raw = sum(raw_scores.values())
        final_scores = {}
        if total_raw > 0:
            for tag, score in raw_scores.items():
                final_scores[tag] = round(score / total_raw, 4)
        
        return grouped, final_scores

def main():
    parser = argparse.ArgumentParser(description="FOA Ingestion Pipeline (ISSR4)")
    parser.add_argument("--url", required=True)
    parser.add_argument("--out_dir", required=True)
    args = parser.parse_args()

    engine = ExtractionEngine(args.url)
    engine.fetch()
    raw_fields = engine.extract_fields()
    
    tagger = SemanticTagger()
    grouped_tags, tag_scores = tagger.group_tags(engine.clean_text)
    raw_fields["tags"] = grouped_tags
    raw_fields["tag_scores"] = tag_scores

    record = FOARecord(**raw_fields)
    response = FOAResponse(data=record)

    os.makedirs(args.out_dir, exist_ok=True)
    with open(os.path.join(args.out_dir, "foa.json"), 'w', encoding='utf-8') as f:
        json.dump(response.model_dump(), f, indent=4, ensure_ascii=False)
        
    with open(os.path.join(args.out_dir, "foa.csv"), 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=[f for f in record.model_dump().keys() if f != "tags"] + ["tags_metadata"])
        writer.writeheader()
        csv_data = record.model_dump()
        tags_flat = [t for sublist in csv_data['tags'].values() for t in sublist]
        csv_data['tags_metadata'] = "|".join(tags_flat)
        del csv_data['tags']
        csv_data['tag_scores'] = json.dumps(csv_data['tag_scores'])
        writer.writerow(csv_data)

    table = Table(title=f"Extraction Success: {record.foa_id}", show_header=True, header_style="bold green")
    table.add_column("Field", style="cyan"); table.add_column("Value", style="white")
    table.add_row("Agency", record.agency)
    table.add_row("Score Sum", f"{sum(record.tag_scores.values()):.2f}")
    console.print(table)

if __name__ == "__main__":
    main()
