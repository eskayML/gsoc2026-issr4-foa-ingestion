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
    schema_version: str = "1.2.0"
    extractor_engine: str = "ISSR4-MultiSource"

class FOARecord(BaseModel):
    foa_id: str
    title: str
    agency: str
    open_date: Optional[str]
    close_date: Optional[str]
    eligibility: str
    program_description: str
    award_ceiling: Optional[int] = None
    award_floor: Optional[int] = None
    source_url: str
    tags: List[str]
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
            console.print(f"[bold red]❌ Error: Failed to fetch {self.url} ({e})[/bold red]")
            raise
        
        self.soup = BeautifulSoup(self.raw_html, 'html.parser')
        extracted = trafilatura.extract(self.raw_html)
        self.clean_text = extracted if extracted else self.soup.get_text(separator=' ', strip=True)

    def parse_currency(self, text: str) -> Optional[int]:
        matches = re.findall(r'\$\s*([\d,]+)', text)
        if matches:
            try:
                amounts = [int(m.replace(',', '')) for m in matches]
                return max(amounts)
            except: pass
        return None

    def extract_fields(self) -> dict:
        title = "Unknown FOA Title"
        title_tag = self.soup.find('title')
        if title_tag:
            title = title_tag.text.strip()
        
        foa_id_match = re.search(r'([A-Z]+[\s-]*\d{2}-\d{3})', self.clean_text)
        foa_id = foa_id_match.group(0).replace(' ', '') if foa_id_match else f"FOA-{int(datetime.now().timestamp())}"
        
        agency = "US Government Agency"
        if "nsf.gov" in self.url.lower():
            agency = "National Science Foundation"
        elif "grants.gov" in self.url.lower():
            agency = "Grants.gov (Federal)"

        date_matches = re.findall(r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}', self.clean_text)
        open_date = date_matches[0] if len(date_matches) > 0 else None
        close_date = date_matches[-1] if len(date_matches) > 1 else None
        
        def to_iso(d_str):
            if not d_str: return None
            try:
                return datetime.strptime(d_str, "%B %d, %Y").strftime("%Y-%m-%d")
            except: return None

        eligibility = "Not specified"
        elig_idx = self.clean_text.lower().find("eligibility")
        if elig_idx != -1:
            eligibility = self.clean_text[elig_idx:elig_idx+500].strip()
        
        return {
            "foa_id": foa_id,
            "title": title,
            "agency": agency,
            "open_date": to_iso(open_date),
            "close_date": to_iso(close_date),
            "eligibility": eligibility,
            "program_description": self.clean_text[:3000],
            "award_ceiling": self.parse_currency(self.clean_text),
            "award_floor": None,
            "source_url": self.url,
        }

# --- Semantic Tagger ---
class SemanticTagger:
    def __init__(self, ontology_path="ontology.json"):
        with open(ontology_path, 'r') as f:
            self.ontology = json.load(f)

    def score_text(self, text: str) -> Dict[str, float]:
        text_lower = text.lower()
        scores = {}
        for category, tags in self.ontology.items():
            for tag_name, tag_data in tags.items():
                hits = sum(1 for kw in tag_data['keywords'] if kw in text_lower)
                if hits > 0:
                    base_score = min(1.0, (hits * 0.3)) * tag_data['weight']
                    scores[tag_name] = round(base_score, 3)
        return scores

def main():
    parser = argparse.ArgumentParser(description="FOA Ingestion Pipeline")
    parser.add_argument("--url", required=True, help="URL of the FOA to ingest")
    parser.add_argument("--out_dir", required=True, help="Output directory")
    args = parser.parse_args()

    engine = ExtractionEngine(args.url)
    engine.fetch()
    raw_fields = engine.extract_fields()
    
    tagger = SemanticTagger()
    tag_scores = tagger.score_text(engine.clean_text)
    raw_fields["tag_scores"] = tag_scores
    raw_fields["tags"] = list(tag_scores.keys())

    record = FOARecord(**raw_fields)
    response = FOAResponse(data=record)

    os.makedirs(args.out_dir, exist_ok=True)
    json_path = os.path.join(args.out_dir, "foa.json")
    csv_path = os.path.join(args.out_dir, "foa.csv")
    
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(response.model_dump(), f, indent=4)
        
    with open(csv_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=record.model_dump().keys())
        writer.writeheader()
        csv_data = record.model_dump()
        csv_data['tags'] = "|".join(csv_data['tags'])
        csv_data['tag_scores'] = json.dumps(csv_data['tag_scores'])
        writer.writerow(csv_data)

    table = Table(title=f"FOA Extraction Summary: {record.foa_id}", show_header=True, header_style="bold green")
    table.add_column("Field", style="cyan", width=20)
    table.add_column("Value", style="white")
    table.add_row("Title", record.title[:70] + "...")
    table.add_row("Agency", record.agency)
    table.add_row("Tags", ", ".join(record.tags[:5]))
    console.print(table)

if __name__ == "__main__":
    main()
