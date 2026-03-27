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
    schema_version: str = "2.0.0"
    extractor_engine: str = "ISSR4-Modular-Provider-System"

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

# --- Modular Provider Architecture ---

class BaseExtractor:
    """Interface for portal-specific extraction logic"""
    def extract(self, html: str, clean_text: str) -> dict:
        raise NotImplementedError

class NSFExtractor(BaseExtractor):
    def extract(self, html: str, clean_text: str) -> dict:
        # NSF specific anchors
        desc_match = re.search(r'(?i)(Synopsis of Program|Program Description)[:\s]+(.*?)(?=\nIII\.|\nAward Information|$)', clean_text, re.DOTALL)
        description = desc_match.group(2).strip() if desc_match else clean_text
        
        elig_match = re.search(r'(?i)Who May Submit Proposals:(.*?)(?=Who May Serve as PI|V\. Proposal|$)', clean_text, re.DOTALL)
        eligibility = elig_match.group(1).strip() if elig_match else "Refer to NSF solicitation text."
        
        return {
            "agency": "National Science Foundation (NSF)",
            "description": description,
            "eligibility": eligibility
        }

class GrantsGovExtractor(BaseExtractor):
    def extract(self, html: str, clean_text: str) -> dict:
        # Grants.gov specific patterns (often NIH/DOE/etc)
        return {
            "agency": "Grants.gov Portal (Federal)",
            "description": clean_text,
            "eligibility": "Refer to Grants.gov opportunity details."
        }

class ExtractionEngine:
    def __init__(self, url: str):
        self.url = url
        self.raw_html = ""
        self.clean_text = ""
        self.soup = None
        # Dynamic Provider Selection
        if "nsf.gov" in url.lower():
            self.provider = NSFExtractor()
        else:
            self.provider = GrantsGovExtractor()

    def fetch(self):
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        try:
            response = requests.get(self.url, headers=headers, timeout=15)
            response.raise_for_status()
            self.raw_html = response.text
        except Exception as e:
            console.print(f"[bold red]❌ Network Error: {self.url} ({e})[/bold red]")
            raise
        
        self.soup = BeautifulSoup(self.raw_html, 'html.parser')
        extracted = trafilatura.extract(self.raw_html, include_tables=True)
        self.clean_text = extracted if extracted else self.soup.get_text(separator='\n', strip=True)

    def sanitize(self, text: str) -> str:
        if not text: return ""
        text = "".join(ch for ch in text if ch.isprintable() or ch in "\n\t")
        return " ".join(text.replace('\\', '/').replace('"', "'").split())

    def extract_fields(self) -> dict:
        # Common Extraction Logic
        title = self.soup.find('title').text.strip() if self.soup.find('title') else "FOA Document"
        foa_id_match = re.search(r'([A-Z]+[\s-]*\d{2}-\d{3})', self.clean_text)
        foa_id = foa_id_match.group(0).replace(' ', '') if foa_id_match else f"FOA-{int(datetime.now().timestamp())}"
        
        date_matches = re.findall(r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}', self.clean_text)
        
        def to_iso(d_str):
            try: return datetime.strptime(d_str, "%B %d, %Y").strftime("%Y-%m-%d")
            except: return None

        # Provider-Specific logic
        p_data = self.provider.extract(self.raw_html, self.clean_text)
        
        # Award Range Logic (Shared)
        matches = re.findall(r'\$\s*([\d,]+)', self.clean_text)
        award_str = "Not specified"
        if matches:
            vals = [int(re.sub(r'[,.]', '', m)) for m in matches if int(re.sub(r'[,.]', '', m)) > 1000]
            if len(vals) >= 2: award_str = f"${min(vals):,} - ${max(vals):,}"
            elif vals: award_str = f"Up to ${vals[0]:,}"

        return {
            "foa_id": foa_id,
            "title": self.sanitize(title),
            "agency": p_data["agency"],
            "open_date": to_iso(date_matches[0]) if len(date_matches) > 0 else None,
            "close_date": to_iso(date_matches[-1]) if len(date_matches) > 1 else None,
            "eligibility": self.sanitize(p_data["eligibility"])[:2000],
            "program_description": self.sanitize(p_data["description"]),
            "award_range": award_str,
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
        
        total_raw = sum(raw_scores.values())
        final_scores = {}
        if total_raw > 0:
            for tag, score in raw_scores.items():
                final_scores[tag] = round(score / total_raw, 4)
        return grouped, final_scores

def main():
    parser = argparse.ArgumentParser(description="Modular FOA Ingestion Engine")
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
    table.add_row("Provider", engine.provider.__class__.__name__)
    table.add_row("Agency", record.agency)
    table.add_row("Award", record.award_range)
    console.print(table)

if __name__ == "__main__":
    main()
