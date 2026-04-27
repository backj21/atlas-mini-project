# ATLAS STAT PROJECT
## Introduction
Apartment hunting is one of the most difficult and stressful parts of student life. While many websites list available rental properties, most of them are designed to advertise apartments rather than evaluate them honestly. As a result, negative experiences can be hidden, filtered, or difficult to find, leaving students without a clear picture of what living in a specific apartment is actually like. Important information about property management, maintenance responsiveness, hidden fees, and lease conditions is also often scattered or overlooked.

Our team aims to address this problem by building a data-driven website that presents a more candid view of apartments near the University of Illinois Urbana-Champaign. By collecting and analyzing data from Reddit, Google Reviews, apartment listing websites, and MTD commute information, we want to help students compare housing options based on both structured data and real student experiences. Our goal is to provide a transparent, evidence-based review of each apartment so students can make more informed housing decisions.

## Current Apartments List
1. HUB
2. HERE
3. Dean
4. Icon
5. Latitude
6. Octave
7. Seven07
8. Tower At Third
9. Yugo
10. Armory 75
11. Illini Manor

## Repository structure
- `frontend/`: React/Vite UI application.
- `backend/`: backend service scaffold (`database/`, `models/`, `routers/`, `schemas/`).
- `scripts/`: runnable data scripts.
  - `scripts/scraping/reddit/`
  - `scripts/scraping/google_maps/`
  - `scripts/scraping/commute_matrix/`
  - `scripts/sentiment/`
- `data/`: project datasets.
  - `data/commute/`: commute-time related datasets
  - `data/google_reviews/`: Google review raw and processed files
  - `data/reddit/`: Reddit raw, processed, outputs, and manual files
  - `data/apartments/`: apartment datasets including `seyeon_atlas_apartments_scoring`
- `analysis/`: notebooks and generated figures.
## Common script entry points
- Reddit pipeline: `python scripts/scraping/reddit/reddit_scraper_pipeline.py`
- Reddit JSON scraper: `python scripts/scraping/reddit/reddit_json_scraper.py`
- VADER sentiment pass: `python scripts/sentiment/analyze_vader.py <input_csv>`
- Commute matrix builder: `python scripts/scraping/commute_matrix/build_commute_matrix.py`
- Google Maps parser: `python scripts/scraping/google_maps/parse_google_maps_reviews.py`
