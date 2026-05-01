# ATLAS / IlliniNest
IlliniNest is a UIUC student housing research project. The project combines apartment pricing, amenities, commute times, Google reviews, and Reddit discussions into one place so students can compare apartments with source-backed evidence.

This repository contains both:
- a static web frontend (`frontend/`)
- data collection and data transformation scripts (`scripts/`)

The current app focuses on a curated set of 11 apartments in Champaign-Urbana.

## What the website does
- Shows a ranked apartment list with trust/value context.
- Lets users compare walk/bike commute times to campus buildings.
- Includes an interactive map for apartment and campus context.
- Displays apartment-level detail panels with:
  - pricing summary
  - amenities
  - commute summary
  - selected positive/negative review highlights from Google and Reddit
- Includes a methodology page explaining scoring inputs and project limits.

## Current apartment coverage
- HERE Champaign
- Hub on Campus Champaign
- The Dean Campustown
- ICON Apartments
- Latitude Apartments
- Octave
- Seven07
- The Tower at Third
- Yugo Urbana Illinois
- 75 Armory
- Illini Manor Apartments

## Repository layout
- `frontend/`: React + Vite website (main product surface).
- `backend/`: backend scaffold (`database/`, `models/`, `routers/`, `schemas/`), not currently active in runtime flow.
- `scripts/`: data collection and processing utilities.
  - `scripts/scraping/reddit/`
  - `scripts/scraping/google_maps/`
  - `scripts/scraping/commute_matrix/`
  - `scripts/sentiment/`
  - `scripts/build_apartment_details.py`
- `data/`: project datasets grouped by domain.
  - `data/apartments/`
  - `data/commute/`
  - `data/google_reviews/`
  - `data/reddit/`
- `analysis/`: exploratory notebooks and generated figures.
- `APARTMENT_ID_REFERENCE.md`: canonical `apartment_id` mapping and alias rules.

## Architecture and data flow
At runtime, the frontend is static and reads prebuilt JSON.

Flow:
1. Raw or intermediate CSV files are stored under `data/`.
2. `scripts/build_apartment_details.py` merges those sources into one frontend-ready JSON file.
3. The script writes `frontend/src/app/data/apartmentDetails.json`.
4. React components read that JSON and render the app.

Important note:
- For normal frontend development, you do not need to run all scraping scripts.
- You only need to regenerate `apartmentDetails.json` when source CSVs change.

## Running the project locally
From the repository root:

```bash
cd frontend
npm install
npm run dev
```

Production build:

```bash
cd frontend
npm run build
```

## Regenerating frontend apartment data
When files in `data/` change, regenerate the JSON bundle used by the app:

```bash
python3 scripts/build_apartment_details.py
```

Output:
- `frontend/src/app/data/apartmentDetails.json`

## Data script entry points
- Reddit pipeline:
  - `python scripts/scraping/reddit/reddit_scraper_pipeline.py`
  - Writes raw/processed outputs under `data/reddit/`.
- Single-pass Reddit JSON scrape:
  - `python scripts/scraping/reddit/reddit_json_scraper.py`
- VADER scoring utility:
  - `python scripts/sentiment/analyze_vader.py <input_csv>`
- Google Maps parser:
  - `python scripts/scraping/google_maps/parse_google_maps_reviews.py`
- Commute matrix builder:
  - `python scripts/scraping/commute_matrix/build_commute_matrix.py`

## Canonical apartment IDs
All processed files should use the same `apartment_id` keys. Use:
- `APARTMENT_ID_REFERENCE.md`

Do not invent new IDs or keep source-specific aliases in processed outputs.

## Frontend implementation notes
- Main app entry: `frontend/src/app/App.tsx`
- Primary routes are state-driven (`home`, `browse`, `map`, `methodology`)
- Key components live in `frontend/src/app/components/`
- Map rendering uses Leaflet via `react-leaflet`
- Styling uses project tokens in `frontend/src/styles/`

## Known limitations (current prototype)
- Coverage is limited to a curated apartment subset.
- Data freshness depends on script runs and source refresh cadence.
- Sentiment scoring is heuristic and should be interpreted as directional signal, not ground truth.

## Contributing
If you add new apartments or data sources:
1. Update canonical IDs in `APARTMENT_ID_REFERENCE.md`.
2. Update script mappings in `scripts/build_apartment_details.py`.
3. Regenerate `frontend/src/app/data/apartmentDetails.json`.
4. Verify frontend behavior with `npm run build` in `frontend/`.
