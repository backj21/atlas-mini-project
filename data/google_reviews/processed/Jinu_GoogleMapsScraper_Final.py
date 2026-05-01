import json
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

matplotlib.rcParams["figure.dpi"] = 120

# VADER 감정 분석기 초기화
analyzer = SentimentIntensityAnalyzer()

# ── Apartment ID 매핑
name_to_id = {
    "HERE Champaign":                   "here",
    "Here":                             "here",
    "HERE":                             "here",
    "Hub on Campus Champaign - Daniel": "hub",
    "Hub on Campus Champaign":          "hub",
    "Hub on Campus":                    "hub",
    "Hub":                              "hub",
    "The Dean Campustown":              "dean",
    "The Dean":                         "dean",
    "Dean":                             "dean",
    "ICON Apartments":                  "icon",
    "ICON":                             "icon",
    "Icon":                             "icon",
    "Latitude Apartments":              "latitude",
    "Latitude":                         "latitude",
    "Octave":                           "octave",
    "Seven07":                          "seven07",
    "Seven 07":                         "seven07",
    "707":                              "seven07",
    "The Tower at Third":               "tower_at_third",
    "Tower at Third":                   "tower_at_third",
    "T3":                               "tower_at_third",
    "Yugo Urbana Illinois":             "yugo",
    "Yugo Urbana":                      "yugo",
    "Yugo":                             "yugo",
    "75 Armory":                        "armory_75",
    "75 E Armory":                      "armory_75",
    "Seventy Five Armory":              "armory_75",
    "Illini Manor Apartments":          "illini_manor",
    "Illini Manor":                     "illini_manor",
}

# JSON 파일 읽기
with open("Jinu_GoogleMapsScraper_ALL.json", "r", encoding="utf-8") as f:
    data = json.load(f)

# 필요한 컬럼만 추출
rows = []
for item in data:
    rows.append({
        "apartment":      item.get("title"),
        "stars":          item.get("stars"),
        "review_text":    item.get("text"),
        "date":           item.get("publishedAtDate"),
        "reviewer_name":  item.get("name"),
        "owner_response": item.get("responseFromOwnerText"),
    })

df = pd.DataFrame(rows).drop_duplicates()

# apartment_id 컬럼 추가
df["apartment_id"] = df["apartment"].map(name_to_id)

# 매핑 안 된 것 확인
unresolved = df[df["apartment_id"].isna()]["apartment"].unique()
if len(unresolved) > 0:
    print(f"⚠️  Unresolved apartments (not mapped to any apartment_id): {unresolved}")

# VADER 감정 점수 추가
df["sentiment_score"] = df["review_text"].apply(
    lambda x: analyzer.polarity_scores(str(x))["compound"]
)
df["sentiment_label"] = df["sentiment_score"].apply(
    lambda x: "positive" if x >= 0.05 else ("negative" if x <= -0.05 else "neutral")
)

# apartment_id를 맨 앞으로
cols = ["apartment_id"] + [c for c in df.columns if c != "apartment_id"]
df = df[cols]

# 결과 확인
print(f"총 리뷰 수: {len(df)}")
print("\n아파트별 리뷰 수:")
print(df["apartment"].value_counts())
print("\n아파트별 평균 별점:")
print(df.groupby("apartment")["stars"].mean().sort_values(ascending=False).round(2))
print("\n아파트별 평균 VADER 감정 점수:")
print(df.groupby("apartment")["sentiment_score"].mean().sort_values(ascending=False).round(2))
print("\n아파트별 감정 분포 (positive/neutral/negative):")
print(df.groupby(["apartment", "sentiment_label"]).size().unstack(fill_value=0))

# CSV로 저장
df.to_csv("Jinu_GoogleMapsScraper_ALL.csv", index=False, encoding="utf-8-sig")
print("\n저장 완료: Jinu_GoogleMapsScraper_ALL.csv")

# ───────────────────────────────────────────
# 그래프
# ───────────────────────────────────────────
apt_order = df.groupby("apartment")["stars"].mean().sort_values(ascending=False).index

fig, axes = plt.subplots(3, 1, figsize=(9, 11))
fig.suptitle("Google Maps Review Analysis", fontsize=13, fontweight="bold", y=0.99)

# ── 그래프 1: 평균 별점
avg_stars = df.groupby("apartment")["stars"].mean().reindex(apt_order)
bars1 = axes[0].barh(apt_order[::-1], avg_stars[::-1], color="#378ADD", height=0.6)
axes[0].set_title("Average Star Rating", fontsize=11)
axes[0].set_xlim(0, 5)
axes[0].tick_params(axis='y', labelsize=9)
for bar, val in zip(bars1, avg_stars[::-1]):
    axes[0].text(bar.get_width() + 0.05, bar.get_y() + bar.get_height()/2,
                 f"{val:.2f}", va="center", fontsize=8)

# ── 그래프 2: 평균 VADER 감정 점수
avg_sentiment = df.groupby("apartment")["sentiment_score"].mean().reindex(apt_order)
colors2 = ["#E24B4A" if v < 0.3 else "#EF9F27" if v < 0.5 else "#1D9E75" for v in avg_sentiment[::-1]]
bars2 = axes[1].barh(apt_order[::-1], avg_sentiment[::-1], color=colors2, height=0.6)
axes[1].set_title("Average VADER Sentiment Score", fontsize=11)
axes[1].set_xlim(-1, 1)
axes[1].axvline(0, color="gray", linewidth=0.8, linestyle="--")
axes[1].tick_params(axis='y', labelsize=9)
for bar, val in zip(bars2, avg_sentiment[::-1]):
    axes[1].text(bar.get_width() + 0.02, bar.get_y() + bar.get_height()/2,
                 f"{val:.2f}", va="center", fontsize=8)

# ── 그래프 3: 감정 분포
dist = df.groupby(["apartment", "sentiment_label"]).size().unstack(fill_value=0).reindex(apt_order)
dist_pct = dist.div(dist.sum(axis=1), axis=0) * 100
left_vals = {apt: 0 for apt in apt_order[::-1]}
color_map = {"positive": "#1D9E75", "neutral": "#EF9F27", "negative": "#E24B4A"}
for label in ["positive", "neutral", "negative"]:
    if label in dist_pct.columns:
        vals = dist_pct[label].reindex(apt_order[::-1]).values
        lefts = [left_vals[apt] for apt in apt_order[::-1]]
        axes[2].barh(apt_order[::-1], vals, left=lefts, color=color_map[label], label=label, height=0.6)
        for apt, v in zip(apt_order[::-1], vals):
            left_vals[apt] += v
axes[2].set_title("Sentiment Distribution (%)", fontsize=11)
axes[2].set_xlim(0, 100)
axes[2].tick_params(axis='y', labelsize=9)
axes[2].legend(loc="lower right", fontsize=8)

plt.tight_layout()
plt.savefig("Jinu_GoogleMaps_graphs.png", bbox_inches="tight")
print("그래프 저장 완료: Jinu_GoogleMaps_graphs.png")
plt.show()
