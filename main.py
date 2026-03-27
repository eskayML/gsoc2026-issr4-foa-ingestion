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

# --- Pydantic Schema Definition (Optimized for Database Readiness) ---
class FOAMetadata(BaseModel):
    generated_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat() + "Z")
    schema_version: str = "1.5.0"
    extractor_engine: str = "ISSR4-NSF-Primary"

class FOARecord(BaseModel):
    foa_id: str
    title: str
    agency: str
    open_date: Optional[str] # ISO YYYY-MM-DD
    close_date: Optional[str] # ISO YYYY-MM-DD
    eligibility: str
    program_description: str # Full high-fidelity text
    award_range: str # Formatted as "$min - $max"
    source_url: str
    tags: Dict[str, List[str]] # Categorized per ISSR ontology
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
        extracted = trafilatura.extract(self.raw_html)
        # Use high-fidelity extraction but preserve core formatting
        self.clean_text = extracted if extracted else self.soup.get_text(separator=' ', strip=True)

    def extract_award_range(self) -> str:
        # Extract digits and clean currency for database normalization
        matches = re.findall(r'\$\s*([\d,]+)', self.clean_text)
        if len(matches) >= 2:
            try:
                vals = [int(m.replace(',', '')) for m in matches]
                return f"${min(vals):,} - ${max(vals):,}"
            except: pass
        elif len(matches) == 1:
            return f"Up to ${matches[0]}"
        return "Not specified"

    def extract_fields(self) -> dict:
        title = self.soup.find('title').text.strip() if self.soup.find('title') else "FOA Document"
        
        # ID extraction matching NSF/NIH/Standard patterns
        foa_id_match = re.search(r'([A-Z]+[\s-]*\d{2}-\d{3})', self.clean_text)
        foa_id = foa_id_match.group(0).replace(' ', '') if foa_id_match else f"FOA-INGEST-{int(datetime.now().timestamp())}"
        
        agency = "US Government Agency"
        if "nsf.gov" in self.url.lower():
            agency = "National Science Foundation (NSF)"
        elif "grants.gov" in self.url.lower():
            agency = "Grants.gov / Federal"

        # Regex for common US Date formats in government docs
        date_matches = re.findall(r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}', self.clean_text)
        
        def to_iso(d_str):
            if not d_str: return None
            try:
                return datetime.strptime(d_str, "%B %d, %Y").strftime("%Y-%m-%d")
            except: return None

        # Eligibility extraction
        eligibility = "Refer to source URL."
        elig_match = re.search(r'(?i)eligibility\s*information[:\s]+(.*?)(?=\n\n|\n[A-Z][a-z]+ [A-Z]|$)', self.clean_text, re.DOTALL)
        if elig_match:
            eligibility = elig_match.group(1).strip()
        else:
            elig_idx = self.clean_text.lower().find("eligibility")
            if elig_idx != -1:
                eligibility = self.clean_text[elig_idx:elig_idx+1000].strip()

        return {
            "foa_id": foa_id,
            "title": title,
            "agency": agency,
            "open_date": to_iso(date_matches[0]) if len(date_matches) > 0 else None,
            "close_date": to_iso(date_matches[-1]) if len(date_matches) > 1 else None,
            "eligibility": eligibility[:2000], # Clean snippet
            "program_description": self.clean_text, # Full Clean Text for Data Readiness
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
        scores = {}
        for category, tags in self.ontology.items():
            for tag_name, tag_data in tags.items():
                hits = sum(1 for kw in tag_data['keywords'] if kw in text_lower)
                if hits > 0:
                    grouped[category].append(tag_name)
                    # Normalized confidence based on term frequency and weight
                    scores[tag_name] = round(min(1.0, (hits * 0.25)) * tag_data['weight'], 2)
        return grouped, scores

def main():
    parser = argparse.ArgumentParser(description="FOA Ingestion Pipeline (ISSR4)")
    parser.add_argument("--url", required=True, help="URL of the FOA Announcement")
    parser.add_argument("--out_dir", required=True, help="Target output directory")
    args = parser.parse_args()

    console.print(f"\n[bold blue][*] Running Production Pipeline for:[/bold blue] {args.url}")
    
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
    json_path = os.path.join(args.out_dir, "foa.json")
    csv_path = os.path.join(args.out_dir, "foa.csv")
    
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(response.model_dump(), f, indent=4)
        
    with open(csv_path, 'w', encoding='utf-8', newline='') as f:
        # Map fields to CSV (Flattening tags for simple ingest)
        writer = csv.DictWriter(f, fieldnames=[f for f in record.model_dump().keys() if f != "tags"] + ["tags_metadata"])
        writer.writeheader()
        csv_data = record.model_dump()
        tags_flat = [t for sublist in csv_data['tags'].values() for t in sublist]
        csv_data['tags_metadata'] = "|".join(tags_flat)
        del csv_data['tags']
        csv_data['tag_scores'] = json.dumps(csv_data['tag_scores'])
        writer.writerow(csv_data)

    table = Table(title=f"Extraction Success: {record.foa_id}", show_header=True, header_style="bold green")
    table.add_column("Field", style="cyan", width=20); table.add_column("Value", style="white")
    table.add_row("Agency", record.agency)
    table.add_row("Award Range", record.award_range)
    table.add_row("ISO Dates", f"{record.open_date} to {record.close_date}")
    console.print(table)
    console.print(Panel(f"✅ Data Validated & Saved to: {args.out_dir}"))

if __name__ == "__main__":
    main()
