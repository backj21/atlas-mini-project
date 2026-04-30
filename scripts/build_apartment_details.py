#!/usr/bin/env python3
"""Build the static apartment detail dataset used by the demo frontend."""

from __future__ import annotations

import csv
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_PATH = ROOT / "frontend/src/app/data/apartmentDetails.json"

SUPPORTED_APARTMENT_IDS = [
    "icon",
    "latitude",
    "yugo",
    "octave",
    "armory_75",
    "illini_manor",
    "seven07",
    "tower_at_third",
    "here",
    "hub",
    "dean",
]

NAME_TO_ID = {
    "75 armory": "armory_75",
    "707": "seven07",
    "dean": "dean",
    "gregory": "gregory",
    "here": "here",
    "here champaign": "here",
    "hub": "hub",
    "hub on campus champaign": "hub",
    "hub on campus champaign - daniel": "hub",
    "icon": "icon",
    "icon apartments": "icon",
    "illini manor": "illini_manor",
    "illini manor apartments": "illini_manor",
    "latitude": "latitude",
    "latitude apartments": "latitude",
    "octave": "octave",
    "seven07": "seven07",
    "the dean campustown": "dean",
    "the tower at third": "tower_at_third",
    "tower at third": "tower_at_third",
    "t3": "tower_at_third",
    "yugo": "yugo",
    "yugo urbana illinois": "yugo",
}

AMENITY_LABELS = {
    "gym": "Gym",
    "pool": "Pool",
    "study_lounge": "Study lounge",
    "parking": "Parking",
    "rooftopCourtyard": "Rooftop / courtyard",
    "inUnitWasherDryer": "In-unit washer/dryer",
    "elevator": "Elevator",
    "bikeStorage": "Bicycle storage",
    "petAllowed": "Pet allowed",
}


def clean_key(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip().lower())


def apartment_id_for_name(value: str | None) -> str | None:
    if not value:
        return None
    return NAME_TO_ID.get(clean_key(value))


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def parse_float(value: str | None) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(str(value).replace(",", ""))
    except ValueError:
        return None


def parse_int(value: str | None) -> int | None:
    parsed = parse_float(value)
    if parsed is None:
        return None
    return int(parsed)


def parse_bool(value: str | None) -> bool:
    return clean_key(value or "") in {"yes", "true", "1", "y"}


def word_count(text: str) -> int:
    return len(re.findall(r"\b[\w']+\b", text))


def is_meaningful_text(text: str) -> bool:
    normalized = " ".join(text.split())
    if len(normalized) < 35:
        return False
    if word_count(normalized) < 8:
        return False
    return True


def source_link(url: str | None) -> str | None:
    if not url:
        return None
    stripped = url.strip()
    return stripped or None


def compact_text(text: str) -> str:
    return " ".join(text.split())


def excerpt_text(text: str, limit: int = 420) -> str:
    if len(text) <= limit:
        return text
    boundary = max(text.rfind(".", 0, limit), text.rfind("!", 0, limit), text.rfind("?", 0, limit))
    if boundary > limit * 0.55:
        return text[: boundary + 1]
    return text[:limit].rsplit(" ", 1)[0] + "..."


def build_empty_details() -> dict[str, dict[str, Any]]:
    return {
        apartment_id: {
            "amenities": {},
            "scores": {},
            "pricing": {
                "minRent": None,
                "maxRent": None,
                "floorPlanCount": 0,
                "perPerson": False,
                "samplePlans": [],
            },
            "commute": [],
            "reviews": {
                "reddit": {"positive": None, "negative": None},
                "google": {"positive": None, "negative": None},
            },
        }
        for apartment_id in SUPPORTED_APARTMENT_IDS
    }


def add_amenities(details: dict[str, dict[str, Any]]) -> None:
    rows = read_csv(ROOT / "data/apartments/seyeon_atlas_apartments_scoring/uiuc-housing-amenities-final.csv")
    key_map = {
        "gym": "gym",
        "pool": "pool",
        "study_lounge": "study_lounge",
        "parking ": "parking",
        "rooftop / courtyard": "rooftopCourtyard",
        "in_unit_washer/dryer": "inUnitWasherDryer",
        "Elevator": "elevator",
        "Bicycle Storage": "bikeStorage",
        "pet_allowed": "petAllowed",
    }

    for row in rows:
        apartment_id = apartment_id_for_name(row.get("apt_name"))
        if apartment_id not in details:
            continue
        details[apartment_id]["amenities"] = {
            target_key: {
                "label": AMENITY_LABELS[target_key],
                "available": parse_bool(row.get(source_key)),
            }
            for source_key, target_key in key_map.items()
        }


def add_scores(details: dict[str, dict[str, Any]]) -> None:
    rows = read_csv(ROOT / "data/apartments/seyeon_atlas_apartments_scoring/housing_scores_final_final.csv")
    for row in rows:
        apartment_id = row.get("") or row.get("apartment_id")
        if apartment_id not in details:
            continue
        details[apartment_id]["scores"] = {
            "amenityScore": parse_float(row.get("amenity_score")),
            "petScore": parse_float(row.get("pet_score")),
            "priceScore": parse_float(row.get("price_score")),
            "valueBonus": parse_float(row.get("value_bonus")),
            "totalScore": parse_float(row.get("total_score")),
        }


def add_pricing(details: dict[str, dict[str, Any]]) -> None:
    rows = read_csv(ROOT / "data/apartments/seyeon_atlas_apartments_scoring/uiuc-housing-pricing.csv")
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for row in rows:
        apartment_id = apartment_id_for_name(row.get("apt_name"))
        if apartment_id not in details:
            continue
        rent = parse_int(row.get("rent ($)"))
        if rent is None:
            continue
        grouped[apartment_id].append(
            {
                "beds": parse_float(row.get("beds")),
                "baths": parse_float(row.get("baths")),
                "sqft": parse_int(row.get("sqft")),
                "rent": rent,
                "perPerson": parse_bool(row.get("per_person")),
                "notes": row.get("notes", "").strip() or None,
            }
        )

    for apartment_id, plans in grouped.items():
        plans.sort(key=lambda plan: plan["rent"])
        rents = [plan["rent"] for plan in plans]
        details[apartment_id]["pricing"] = {
            "minRent": min(rents),
            "maxRent": max(rents),
            "floorPlanCount": len(plans),
            "perPerson": any(plan["perPerson"] for plan in plans),
            "samplePlans": plans[:3],
        }


def add_commute(details: dict[str, dict[str, Any]]) -> None:
    rows = read_csv(ROOT / "data/commute/processed/commute_matrix.csv")
    for row in rows:
        apartment_id = row.get("apartment_id")
        if apartment_id not in details:
            continue
        details[apartment_id]["commute"].append(
            {
                "building": row.get("building"),
                "walkingMin": parse_int(row.get("walking_min")),
                "bicyclingMin": parse_int(row.get("bicycling_min")),
                "transitMin": parse_int(row.get("transit_min")),
            }
        )


def comment_specificity(text: str) -> int:
    keywords = [
        "maintenance",
        "management",
        "room",
        "bedroom",
        "noise",
        "walls",
        "amenities",
        "elevator",
        "parking",
        "lease",
        "rent",
        "apartment",
        "location",
        "staff",
        "internet",
    ]
    lowered = text.lower()
    return sum(1 for keyword in keywords if keyword in lowered)


def better_positive(candidate: dict[str, Any]) -> tuple[float, int, int]:
    return (candidate["score"], comment_specificity(candidate["text"]), word_count(candidate["text"]))


def better_negative(candidate: dict[str, Any]) -> tuple[float, int, int]:
    return (-candidate["score"], comment_specificity(candidate["text"]), word_count(candidate["text"]))


def add_reddit_reviews(details: dict[str, dict[str, Any]]) -> None:
    rows = read_csv(ROOT / "data/reddit/processed/reddit_housing_processed.csv")
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    seen: set[tuple[str, str]] = set()

    for row in rows:
        apartment_id = clean_key(row.get("Apartment ID", ""))
        if apartment_id not in details:
            continue
        text = compact_text(row.get("Comment Text", ""))
        if not is_meaningful_text(text):
            continue
        dedupe_key = (apartment_id, text.lower())
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        score = parse_float(row.get("vader_score")) or 0
        grouped[apartment_id].append(
            {
                "text": excerpt_text(text),
                "author": row.get("Author", "").strip() or "Reddit user",
                "url": source_link(row.get("Original URL")),
                "score": score,
                "postTitle": row.get("Post Title", "").strip() or None,
            }
        )

    for apartment_id, comments in grouped.items():
        positives = [comment for comment in comments if comment["score"] > 0.15]
        negatives = [comment for comment in comments if comment["score"] < -0.15]
        details[apartment_id]["reviews"]["reddit"] = {
            "positive": max(positives, key=better_positive) if positives else None,
            "negative": max(negatives, key=better_negative) if negatives else None,
        }


def better_google_positive(candidate: dict[str, Any]) -> tuple[float, int, int, int]:
    return (
        candidate["rating"],
        comment_specificity(candidate["text"]),
        word_count(candidate["text"]),
        candidate["likesCount"],
    )


def better_google_negative(candidate: dict[str, Any]) -> tuple[float, int, int, int]:
    return (
        -candidate["rating"],
        comment_specificity(candidate["text"]),
        word_count(candidate["text"]),
        candidate["likesCount"],
    )


def add_google_reviews(details: dict[str, dict[str, Any]]) -> None:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    seen_review_ids: set[str] = set()
    seen_text: set[tuple[str, str]] = set()

    for path in sorted((ROOT / "data/google_reviews/raw").glob("*.csv")):
        for row in read_csv(path):
            apartment_id = apartment_id_for_name(row.get("title"))
            if apartment_id not in details:
                continue
            text = compact_text(row.get("text", ""))
            if not is_meaningful_text(text):
                continue
            review_id = row.get("reviewId", "").strip()
            if review_id and review_id in seen_review_ids:
                continue
            dedupe_key = (apartment_id, text.lower())
            if dedupe_key in seen_text:
                continue
            if review_id:
                seen_review_ids.add(review_id)
            seen_text.add(dedupe_key)
            rating = parse_float(row.get("stars"))
            if rating is None:
                continue
            grouped[apartment_id].append(
                {
                    "text": excerpt_text(text),
                    "author": row.get("name", "").strip() or "Google reviewer",
                    "rating": rating,
                    "date": row.get("publishedAtDate", "").strip() or None,
                    "likesCount": parse_int(row.get("likesCount")) or 0,
                    "url": source_link(row.get("reviewUrl")),
                }
            )

    for apartment_id, comments in grouped.items():
        positives = [comment for comment in comments if comment["rating"] >= 4]
        negatives = [comment for comment in comments if comment["rating"] <= 2]
        details[apartment_id]["reviews"]["google"] = {
            "positive": max(positives, key=better_google_positive) if positives else None,
            "negative": max(negatives, key=better_google_negative) if negatives else None,
        }


def main() -> None:
    details = build_empty_details()
    add_amenities(details)
    add_scores(details)
    add_pricing(details)
    add_commute(details)
    add_reddit_reviews(details)
    add_google_reviews(details)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", encoding="utf-8") as handle:
        json.dump(details, handle, indent=2, ensure_ascii=True)
        handle.write("\n")

    print(f"Wrote {OUTPUT_PATH.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
