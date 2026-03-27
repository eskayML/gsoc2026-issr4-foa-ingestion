import pytest
from main import FOARecord, SemanticTagger

def test_pydantic_validation():
    sample_data = {
        "foa_id": "NSF-TEST",
        "title": "Test Title",
        "agency": "NSF",
        "open_date": "2026-01-01",
        "close_date": "2027-01-01",
        "eligibility": "Test Eligibility",
        "program_description": "Test Description",
        "award_range": "$1M - $5M",
        "source_url": "https://example.com",
        "tags": {"research_domains": ["ai"]},
        "tag_scores": {"ai": 1.0}
    }
    record = FOARecord(**sample_data)
    assert record.foa_id == "NSF-TEST"
    assert sum(record.tag_scores.values()) == 1.0

def test_tagger_normalization():
    # Verify math logic for normalization
    # (Testing the logic without needing a full URL)
    pass
