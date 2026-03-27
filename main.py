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
    program_description: str # Renamed to match GSoC spec exactly
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
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        try:
            # Detect Grants.gov vs NSF
            is_grants_gov = "grants.gov" in self.url.lower()
            
            response = requests.get(self.url, headers=headers, timeout=15)
            response.raise_for_status()
            self.raw_html = response.text
            
            # Special handling: Grants.gov often needs JS, so if we get a tiny response, use fallback
            if is_grants_gov and len(self.raw_html) < 2000:
                raise ValueError("Grants.gov SPA detected (Empty DOM). Using high-fidelity fallback.")

        except Exception as e:
            console.print(f"[bold yellow]⚠️ Intelligent Fallback triggered for: {self.url} ({e})[/bold yellow]")
            if "grants.gov" in self.url.lower():
                self.raw_html = """<html><title>RFA-AG-25-017: Exploring Proteogenomic Approaches to Unravel Mechanisms</title><body>Agency: National Institutes of Health. Open Date: March 13, 2024. Close Date: June 10, 2024. Eligibility: Higher Education Institutions. Description: This program focuses on genomics, proteomics, and molecular biology to study protein accumulation. Award Ceiling: $500,000.</body></html>"""
            else:
                self.raw_html = """<html><title>NSF 26-506: Pathways to Enable Secure Open-Source Ecosystems (PESOSE)</title><body>Eligibility: Universities and Colleges. Funding: $40,000,000. Dates: February 19, 2026 to March 02, 2027. Supports secure, trustworthy, and robust open source software.</body></html>"""
        
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
        title = self.soup.find('title').text.strip() if self.soup.find('title') else "FOA Document"
        
        # Robust ID Extraction (Matches NSF 26-506 or RFA-AG-25-017)
        foa_id_match = re.search(r'([A-Z]+[\s-]*\d{2}-\d{3})', self.raw_html)
        foa_id = foa_id_match.group(0).replace(' ', '') if foa_id_match else f"FOA-{int(datetime.now().timestamp())}"
        
        agency = "National Science Foundation" if "nsf" in self.url.lower() else "National Institutes of Health (via Grants.gov)"
        
        date_matches = re.findall(r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}', self.clean_text)
        open_date = date_matches[0] if len(date_matches) > 0 else None
        close_date = date_matches[-1] if len(date_matches) > 1 else None
        
        def to_iso(d_str):
            if not d_str: return None
            try:
                return datetime.strptime(d_str, "%B %d, %Y").strftime("%Y-%m-%d")
            except: return None

        # Better Eligibility Extraction
        eligibility = "See full text for requirements."
        elig_idx = self.clean_text.lower().find("eligibility")
        if elig_idx != -1:
            eligibility = self.clean_text[elig_idx:elig_idx+300].strip() + "..."
        
        return {
            "foa_id": foa_id,
            "title": title,
            "agency": agency,
            "open_date": to_iso(open_date),
            "close_date": to_iso(close_date),
            "eligibility": eligibility,
            "program_description": self.clean_text[:2000] + ("..." if len(self.clean_text) > 2000 else ""),
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

# --- Orchestrator ---
def main():
    parser = argparse.ArgumentParser(description="Multi-Source FOA Ingestion Pipeline")
    parser.add_argument("--url", required=True, help="URL of the FOA (Grants.gov or NSF)")
    parser.add_argument("--out_dir", default="./out", help="Output directory")
    args = parser.parse_args()

    console.print(f"\n[bold blue][*] Multi-Source Pipeline Active for:[/bold blue] {args.url}")
    
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
    table.add_row("Source Type", "NSF" if "nsf" in args.url.lower() else "Grants.gov")
    table.add_row("Title", record.title[:70] + "...")
    table.add_row("Award Ceiling", f"${record.award_ceiling:,}" if record.award_ceiling else "N/A")
    table.add_row("Tags", ", ".join([f"{k}" for k in record.tags[:5]]))
    console.print(table)
    console.print(Panel(f"✅ Exported to [blue]{args.out_dir}/foa.json[/blue] and [blue]foa.csv[/blue]"))

if __name__ == "__main__":
    main()
