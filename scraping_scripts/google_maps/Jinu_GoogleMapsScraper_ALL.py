import json
import pandas as pd

# JSON 파일 읽기
with open("Jinu_GoogleMapsScraper_ALL.json", "r", encoding="utf-8") as f:
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
df.to_csv("Jinu_GoogleMapsScraper_ALL.csv", index=False, encoding="utf-8-sig")
print("\n저장 완료: Jinu_GoogleMapsScraper_ALL.csv")
