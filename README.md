# ATLAS STAT PROJECT
## Motive

## Objective

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
