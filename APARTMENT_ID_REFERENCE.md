# Apartment ID Reference

Use these canonical `apartment_id` values across all processed datasets.

Raw scraped files can keep their original apartment names, but every cleaned file in `data/processed/` should include an `apartment_id` column using this list.

## Canonical Apartment IDs

| apartment_id | canonical_name |
| --- | --- |
| `here` | HERE Champaign |
| `hub` | Hub on Campus Champaign |
| `dean` | The Dean Campustown |
| `icon` | ICON Apartments |
| `latitude` | Latitude Apartments |
| `octave` | Octave |
| `seven07` | Seven07 |
| `tower_at_third` | The Tower at Third |
| `yugo` | Yugo Urbana Illinois |
| `armory_75` | 75 Armory |
| `illini_manor` | Illini Manor Apartments |

## Name Mapping And Aliases

Use this table to map names from Google reviews, apartment scoring files, commute data, Reddit mentions, and apartment websites back to the canonical `apartment_id`.

| apartment_id | canonical_name | common aliases / source names |
| --- | --- | --- |
| `here` | HERE Champaign | Here; HERE; HERE Champaign |
| `hub` | Hub on Campus Champaign | Hub; Hub on Campus; Hub on Campus Champaign; Hub on Campus Champaign - Daniel |
| `dean` | The Dean Campustown | Dean; The Dean; The Dean Campustown |
| `icon` | ICON Apartments | ICON; Icon; ICON Apartments |
| `latitude` | Latitude Apartments | Latitude; Latitude Apartments |
| `octave` | Octave | Octave |
| `seven07` | Seven07 | 707; Seven07; Seven 07 |
| `tower_at_third` | The Tower at Third | T3; Tower at Third; The Tower at Third |
| `yugo` | Yugo Urbana Illinois | Yugo; Yugo Urbana; Yugo Urbana Illinois |
| `armory_75` | 75 Armory | 75 Armory; Seventy Five Armory |
| `illini_manor` | Illini Manor Apartments | Illini Manor; Illini Manor Apartments |

## Required Processed File Rule

Every processed CSV or JSON file should use the same key:

```csv
apartment_id,...
```

Examples:

```csv
apartment_id,google_review_count,google_avg_rating
dean,100,3.71
```

```csv
apartment_id,median_rent,amenity_score
dean,1420,0.90
```

```csv
apartment_id,building,walking_min,bicycling_min,transit_min
dean,Grainger Engineering Library,13,4,9
```

## Notes

- Do not create new IDs without updating this file.
- Do not use raw source names like `T3`, `707`, or `Hub on Campus Champaign - Daniel` as IDs.
- If an apartment is mentioned in Reddit data but cannot be confidently mapped to one of these IDs, leave it out of the demo dataset or place it in an unresolved file for review.
