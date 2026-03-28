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
from urllib.parse import urlparse, parse_qs

console = Console()

# --- Pydantic Schema Definition (V2.4.0 - Advanced Extraction) ---
class FOAMetadata(BaseModel):
    generated_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat() + "Z")
    schema_version: str = "2.4.0"
    extractor_engine: str = "ISSR4-Advanced-MultiSource-Engine"

class FOARecord(BaseModel):
    foa_id: str
    title: str
    agency: str
    open_date: Optional[str]
    close_date: Optional[str]
    eligibility: str
    program_description: str
    award_min: Optional[int] = None
    award_max: Optional[int] = None
    award_range: str 
    source_url: str
    tags: Dict[str, List[str]]
    tag_scores: Dict[str, float]

class FOAResponse(BaseModel):
    metadata: FOAMetadata = Field(default_factory=FOAMetadata)
    data: List[FOARecord]

# --- Helper Functions ---
def sanitize_text(text: str) -> str:
    if not text: return ""
    text = "".join(ch for ch in text if ch.isprintable() or ch in "\n\t")
    return " ".join(text.replace('\\', '/').replace('"', "'").split())

def parse_date(date_str: str) -> Optional[str]:
    if not date_str: return None
    try:
        # Try different formats, particularly those used by Grants.gov
        if "EDT" in date_str or "EST" in date_str or "PDT" in date_str or "PST" in date_str:
            # Simple fallback for Grants.gov API dates like "Mar 13, 2024 10:35:19 AM EDT"
            parts = date_str.split()
            if len(parts) >= 3:
                clean_str = f"{parts[0]} {parts[1].replace(',', '')} {parts[2]}"
                return datetime.strptime(clean_str, "%b %d %Y").strftime("%Y-%m-%d")
        return datetime.strptime(date_str, "%B %d, %Y").strftime("%Y-%m-%d")
    except:
        return None

# --- Advanced OOP Modular Provider Architecture ---

class BaseProvider:
    """Abstract base class for all FOA data providers."""
    def __init__(self, url: str):
        self.url = url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) FOA-Intelligence-Bot/1.0'
        })
        self.raw_text = ""

    def fetch_and_extract(self) -> dict:
        raise NotImplementedError("Subclasses must implement fetch_and_extract")

class NSFProvider(BaseProvider):
    """Extraction strategy for NSF using DOM parsing and Trafilatura."""
    def fetch_and_extract(self) -> dict:
        response = self.session.get(self.url, timeout=15)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        extracted = trafilatura.extract(response.text, include_tables=True)
        self.raw_text = extracted if extracted else soup.get_text(separator='\n', strip=True)

        title = soup.find('title').text.strip() if soup.find('title') else "NSF FOA"
        id_match = re.search(r'([A-Z]+[\s-]*\d{2}-\d{3,4})', self.raw_text)
        foa_id = id_match.group(0).replace(' ', '') if id_match else f"NSF-{int(datetime.now().timestamp())}"
        
        date_matches = re.findall(r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}', self.raw_text)
        
        desc_match = re.search(r'(?i)(Synopsis of Program|Program Description)[:\s]+(.*?)(?=\nIII\.|\nAward Information|$)', self.raw_text, re.DOTALL)
        description = desc_match.group(2).strip() if desc_match else self.raw_text[:2000]
        
        elig_match = re.search(r'(?i)Who May Submit Proposals:(.*?)(?=Who May Serve as PI|V\. Proposal|$)', self.raw_text, re.DOTALL)
        eligibility = elig_match.group(1).strip() if elig_match else "Refer to NSF solicitation."

        # Parse awards
        award_min, award_max, award_range = self._parse_currency(self.raw_text)

        return {
            "foa_id": foa_id,
            "title": sanitize_text(title),
            "agency": "National Science Foundation (NSF)",
            "open_date": parse_date(date_matches[0]) if len(date_matches) > 0 else None,
            "close_date": parse_date(date_matches[-1]) if len(date_matches) > 1 else None,
            "eligibility": sanitize_text(eligibility),
            "program_description": sanitize_text(description),
            "award_min": award_min,
            "award_max": award_max,
            "award_range": award_range,
            "source_url": self.url
        }

    def _parse_currency(self, text: str):
        min_val, max_val = None, None
        matches = re.findall(r'\$\s*([\d,]+)', text)
        if matches:
            vals = [int(re.sub(r'[,.]', '', m)) for m in matches if int(re.sub(r'[,.]', '', m)) > 1000]
            if vals:
                min_val, max_val = min(vals), max(vals)
        if min_val and max_val and min_val != max_val:
            return min_val, max_val, f"${min_val:,} - ${max_val:,}"
        elif max_val:
            return None, max_val, f"Up to ${max_val:,}"
        return None, None, "Not specified"


class GrantsGovProvider(BaseProvider):
    """
    Advanced extraction strategy for Grants.gov avoiding SPA DOM scraping.
    Carefully intercepts the backend REST API by extracting the Opportunity ID.
    """
    API_ENDPOINT = "https://apply07.grants.gov/grantsws/rest/opportunity/details"

    def fetch_and_extract(self) -> dict:
        # Extract Opportunity ID from the URL
        opp_id = self._extract_opp_id(self.url)
        if not opp_id:
            raise ValueError(f"Could not extract oppId from Grants.gov URL: {self.url}")

        # Post directly to the Grants.gov details API
        response = self.session.post(self.API_ENDPOINT, data={"oppId": opp_id}, timeout=15)
        response.raise_for_status()
        
        data = response.json()
        synopsis = data.get('synopsis', {})
        forecast = data.get('forecast', {})

        title = data.get('opportunityTitle') or "Untitled"
        foa_id = data.get('opportunityNumber') or opp_id
        agency = synopsis.get('agencyName') or "Grants.gov Portal (Federal)"
        
        # Combine synopsis and forecast descriptions for maximum data density
        desc_parts = []
        if synopsis.get('synopsisDesc'):
            desc_parts.append(synopsis.get('synopsisDesc'))
        if forecast.get('forecastDesc'):
            # Forecast often contains rich HTML, parse it clean
            clean_forecast = BeautifulSoup(forecast.get('forecastDesc'), 'html.parser').get_text(separator=' ', strip=True)
            desc_parts.append(clean_forecast)
            
        description = " | ".join(desc_parts) if desc_parts else ""
        self.raw_text = description # for tagging

        open_date = parse_date(synopsis.get('postingDate') or forecast.get('postingDate'))
        close_date = parse_date(synopsis.get('responseDate') or forecast.get('estApplicationResponseDate'))
        
        eligibility = synopsis.get('applicantEligibilityDesc')
        if not eligibility and forecast.get('applicantEligibilityDesc'):
            eligibility = forecast.get('applicantEligibilityDesc')
        elif not eligibility:
            eligibility = "Not specified"

        # Award parsing natively from the JSON payload
        award_min, award_max, award_range = self._parse_api_currency(synopsis if synopsis.get('awardCeiling') else forecast)

        return {
            "foa_id": str(foa_id),
            "title": sanitize_text(title),
            "agency": sanitize_text(agency),
            "open_date": open_date,
            "close_date": close_date,
            "eligibility": sanitize_text(eligibility),
            "program_description": sanitize_text(description),
            "award_min": award_min,
            "award_max": award_max,
            "award_range": award_range,
            "source_url": self.url
        }

    def _extract_opp_id(self, url: str) -> Optional[str]:
        if '/search-results-detail/' in url:
            parts = url.split('/search-results-detail/')
            if len(parts) == 2:
                return parts[1].strip('/')
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        if 'oppId' in params:
            return params['oppId'][0]
        return None

    def _parse_api_currency(self, synopsis: dict):
        min_val, max_val = None, None
        floor_str = synopsis.get('awardFloor', '')
        if floor_str and str(floor_str).lower() != 'none':
            try: min_val = int(float(str(floor_str).replace(',', '')))
            except: pass
        
        ceiling_str = synopsis.get('awardCeiling', '')
        if ceiling_str and str(ceiling_str).lower() != 'none':
            try: max_val = int(float(str(ceiling_str).replace(',', '')))
            except: pass

        if min_val and max_val and min_val != max_val:
            return min_val, max_val, f"${min_val:,} - ${max_val:,}"
        elif max_val:
            return min_val, max_val, f"Up to ${max_val:,}"
        elif min_val:
            return min_val, max_val, f"Minimum ${min_val:,}"
        return None, None, "Not specified"


# --- Context Factory ---
class EngineFactory:
    @staticmethod
    def get_provider(url: str) -> BaseProvider:
        url_lower = url.lower()
        if "nsf.gov" in url_lower:
            return NSFProvider(url)
        elif "grants.gov" in url_lower:
            return GrantsGovProvider(url)
        else:
            raise ValueError("Unsupported platform URL.")

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
    parser = argparse.ArgumentParser(description="Advanced Multi-Source FOA Ingestion")
    parser.add_argument("--url", required=True, help="URL or comma-separated list of URLs")
    parser.add_argument("--out_dir", required=True)
    args = parser.parse_args()

    urls = [u.strip() for u in args.url.split(',')]
    records = []

    for url in urls:
        console.print(f"[cyan]Initializing Provider for: {url}[/cyan]")
        try:
            provider = EngineFactory.get_provider(url)
            raw_fields = provider.fetch_and_extract()
            
            tagger = SemanticTagger()
            grouped_tags, tag_scores = tagger.group_tags(provider.raw_text)
            raw_fields["tags"] = grouped_tags
            raw_fields["tag_scores"] = tag_scores
            
            records.append(FOARecord(**raw_fields))
            console.print(f"[green]✓ Extracted: {raw_fields['foa_id']}[/green]")
        except Exception as e:
            console.print(f"[bold red]❌ Failed to process {url}: {e}[/bold red]")
            continue

    response = FOAResponse(data=records)

    os.makedirs(args.out_dir, exist_ok=True)
    with open(os.path.join(args.out_dir, "foa.json"), 'w', encoding='utf-8') as f:
        json.dump(response.model_dump(), f, indent=4, ensure_ascii=False)
        
    with open(os.path.join(args.out_dir, "foa.csv"), 'w', newline='', encoding='utf-8') as f:
        if records:
            writer = csv.DictWriter(f, fieldnames=[f for f in records[0].model_dump().keys() if f != "tags"] + ["tags_metadata"])
            writer.writeheader()
            for record in records:
                csv_data = record.model_dump()
                tags_flat = [t for sublist in csv_data['tags'].values() for t in sublist]
                csv_data['tags_metadata'] = "|".join(tags_flat)
                del csv_data['tags']
                csv_data['tag_scores'] = json.dumps(csv_data['tag_scores'])
                writer.writerow(csv_data)

    table = Table(title="Batch Extraction Summary", show_header=True, header_style="bold green")
    table.add_column("FOA ID", style="cyan"); table.add_column("Agency", style="white"); table.add_column("Award Range", style="yellow")
    for record in records:
        table.add_row(record.foa_id, record.agency, record.award_range)
    console.print(table)

if __name__ == "__main__":
    main()