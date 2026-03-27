# 🛠️ GSoC 2026 Engineering Log: ISSR4 Ingestion Pipeline

This document details the advanced engineering decisions made during the development of the FOA Ingestion screening task. 

## 1. Extraction Strategy: The Hybrid Layer Approach
Standard `BeautifulSoup` parsing fails on dynamic government portals. This pipeline implements a **Hybrid Layer** strategy:
- **Layer 1 (Trafilatura):** High-fidelity text extraction using a specialized library designed to strip navigation boilerplate and isolate "main" content.
- **Layer 2 (Context Anchors):** To prevent "LLM hallucination" or Regex noise (like picking up global budget figures as award ranges), we use **Contextual Anchors** (e.g., searching specifically for "Synopsis" or "Eligibility Information" as starting offsets).

## 2. Semantic Logic: Normalized Probabilistic Tagging
Traditional keyword matching is binary (Yes/No). We implemented a **Probabilistic Scoring Engine**:
- **Term Frequency-Inverse Document Frequency (TF-IDF) Influence:** The more times a keyword from a category appears, the higher the weight.
- **Ontology Categorization:** Tags are grouped into the four requested categories (Research Domains, Methods, Populations, Sponsor Themes) at the data layer, not just the display layer.
- **Normalization:** All scores are normalized to sum to **1.0**, making the output ready for direct injection into DiRT (Discovery and Research Tooling) algorithms.

## 3. Reliability: The Pydantic Contract
We treat the FOA data as a **Typed Schema**, not a loose dictionary. 
- **Type Coercion:** All award amounts are processed into standardized currency strings.
- **ISO Enforcement:** Dates are strictly coerced into ISO-8601 to ensure database compatibility.
- **Data Integrity:** String sanitization removes control characters that would break JSON serialization in high-latency network transfers.

## 4. Scalability: The SPA Roadmap
The current architecture uses `requests` for speed. However, to handle **Grants.gov** (which uses a React/Angular SPA frontend), we have designed the `ExtractionEngine` to be **Provider-Agnostic**. 
- In the full GSoC project phase, a `HeadlessBrowserProvider` (Playwright) will be injected to handle JavaScript rendering without changing the core extraction logic.
