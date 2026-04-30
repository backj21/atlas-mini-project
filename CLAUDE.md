# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ATLAS is a UIUC student housing recommendation tool ("IlliniNest") that ranks Champaign-Urbana apartment complexes by a computed "trust score" using scraped Google Maps reviews, Reddit comments, commute data, amenities, and pricing. It is a fully static frontend — no backend server.

## Commands

All frontend commands run from the `frontend/` directory:

```bash
cd frontend
npm run dev      # Start Vite dev server
npm run build    # Production build
```

To regenerate the static data bundle after changing source CSVs:

```bash
python3 scripts/build_apartment_details.py
# Outputs: frontend/src/app/data/apartmentDetails.json
```

Python scripts in `scripts/scraping/` and `scripts/sentiment/` are one-off data collection/processing utilities and do not need to run for normal frontend development.

## Architecture

### Data Flow

Raw data (CSVs in `data/`) -> `scripts/build_apartment_details.py` -> `frontend/src/app/data/apartmentDetails.json` -> React frontend at runtime.

The JSON file is the single source of truth for per-apartment details (amenities, scores, pricing, commute times, review excerpts). It is committed to the repo and only needs regeneration when source CSVs change.

### Frontend Structure

- **`frontend/src/app/App.tsx`** — Root component. Owns all global state: current route, selected campus building, transport mode, selected apartment, and dark mode. Also contains hardcoded `supportedApartments` list (with coordinates and trust scores) and the `commuteTimes` lookup table for the four key campus buildings.

- **Routing** — Custom state-based routing via a `Route` type (`'home' | 'browse' | 'map' | 'methodology'`). React Router is installed but not used for navigation — `setCurrentRoute` is passed down via props.

- **`frontend/src/app/components/`** — Main page-level components: `Masthead`, `HomePage`, `ComplexList`, `ApartmentDetailPanel`, `MapView`, `BuildingSelector`, `MethodologyPage`, `Footer`.

- **`frontend/src/app/components/ui/`** — Shadcn-style primitive components (Button, Card, Dialog, etc.) — treat these as library code and avoid modifying them.

- **Mapping** — `MapView.tsx` uses `react-leaflet` with Leaflet. Map CSS is in `frontend/src/styles/map.css`.

### Styling Conventions

- **CSS variables** in `frontend/src/styles/theme.css` define the design system: `--ink`, `--paper`, `--cream`, `--orange`, `--rule`, etc. Dark mode is applied by toggling the `.dark` class on `<html>`.
- Typography: `--serif` (Newsreader), `--sans` (IBM Plex Sans), `--mono` (IBM Plex Mono).
- Tailwind utility classes are used for layout; inline `style` props with CSS variables are used for brand-specific colors/typography. Do not replace inline styles with Tailwind color utilities — the variables are required for dark mode.

### Data Sources

| Directory | Contents |
|---|---|
| `data/google_reviews/raw/` | Raw Google Maps review CSVs |
| `data/reddit/raw/` | Raw Reddit post/comment CSVs |
| `data/reddit/processed/` | VADER-scored Reddit data |
| `data/apartments/seyeon_atlas_apartments_scoring/` | Amenities, pricing, and scoring CSVs |
| `data/commute/processed/commute_matrix.csv` | Walking/biking/transit times from each apartment to each campus building |

### Adding a New Apartment

1. Add an entry to `supportedApartments` and `commuteTimes` in `App.tsx`.
2. Add the apartment ID to `SUPPORTED_APARTMENT_IDS` and `NAME_TO_ID` in `scripts/build_apartment_details.py`.
3. Ensure the apartment appears in the source CSVs under a recognized name.
4. Run `python3 scripts/build_apartment_details.py` to regenerate the JSON.
