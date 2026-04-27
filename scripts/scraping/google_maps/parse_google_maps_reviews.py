import json
from pathlib import Path
import pandas as pd
INPUT_PATH = (
    Path(__file__).resolve().parents[3]
    / "data"
    / "google_reviews"
    / "raw"
    / "Jinu_GoogleMapsScraper_ALL.json"
)
OUTPUT_PATH = (
    Path(__file__).resolve().parents[3]
    / "data"
    / "google_reviews"
    / "processed"
    / "google_maps_reviews_all.csv"
)

# JSON 파일 읽기
with open(INPUT_PATH, "r", encoding="utf-8") as f:
    data = json.load(f)

# 필요한 컬럼만 추출
rows = []
for item in data:
    rows.append({
        "apartment":        item.get("title"),
        "stars":            item.get("stars"),
        "review_text":      item.get("text"),
        "date":             item.get("publishedAtDate"),
        "reviewer_name":    item.get("name"),
        "owner_response":   item.get("responseFromOwnerText"),
    })

df = pd.DataFrame(rows)

# 중복 제거
df = df.drop_duplicates()

# 결과 확인
print(f"총 리뷰 수: {len(df)}")
print("\n아파트별 리뷰 수:")
print(df["apartment"].value_counts())
print("\n아파트별 평균 별점:")
print(df.groupby("apartment")["stars"].mean().sort_values(ascending=False).round(2))

# CSV로 저장
OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")
print(f"\n저장 완료: {OUTPUT_PATH}")
