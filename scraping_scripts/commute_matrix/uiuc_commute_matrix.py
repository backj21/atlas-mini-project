import requests
import pandas as pd
import time

API_KEY = "AIzaSyBxHV4I6pRx77S5Qh4ZXeGxF5z6G5taMTk"

apartments = {
    "HERE Champaign": "308 E Green St, Champaign, IL 61820",
    "Hub on Campus": "812 S 6th St, Champaign, IL 61820",
    "The Dean Campustown": "708 S 6th St, Champaign, IL 61820",
    "ICON Apartments": "309 E Springfield Ave, Champaign, IL 61820",
    "Yugo Urbana": "410 N Lincoln Ave, Urbana, IL 61801",
    "Latitude Apartments": "608 E University Ave, Champaign, IL 61820",
    "The Tower at Third": "302 E John St, Champaign, IL 61820",
    "Seven07": "707 S 4th St, Champaign, IL 61820",
    "Octave": "210 S 4th St, Champaign, IL 61820",
    "75 Armory": "75 E Armory Ave, Champaign, IL 61820",
    "Illini Manor": "401 E Chalmers St, Champaign, IL 61820",
}

buildings = {
    "Grainger Engineering Library": "1301 W Springfield Ave, Urbana, IL 61801",
    "Siebel Center for CS": "201 N Goodwin Ave, Urbana, IL 61801",
    "Illini Union": "1401 W Green St, Urbana, IL 61801",
    "CRCE": "201 E Peabody Dr, Champaign, IL 61820",
    "ARC": "201 E Peabody Dr, Champaign, IL 61820",
    "Main Library": "1408 W Gregory Dr, Urbana, IL 61801",
    "Wohlers Hall": "1206 S Sixth St, Champaign, IL 61820",
    "Everitt Lab": "1406 W Green St, Urbana, IL 61801",
    "Loomis Lab": "1110 W Green St, Urbana, IL 61801",
    "Lincoln Hall": "702 S Wright St, Urbana, IL 61801",
    "Krannert Art Center": "500 Peabody Dr, Champaign, IL 61820",
}

def get_commute_time(origin, destination, mode):
    url = "https://maps.googleapis.com/maps/api/distancematrix/json"
    params = {
        "origins": origin,
        "destinations": destination,
        "mode": mode,
        "key": API_KEY,
    }
    response = requests.get(url, params=params)
    data = response.json()
    try:
        duration = data["rows"][0]["elements"][0]["duration"]["value"]
        return round(duration / 60)  # 초 → 분
    except:
        return None

rows = []
modes = ["walking", "transit", "bicycling"]

for apt_name, apt_addr in apartments.items():
    for bld_name, bld_addr in buildings.items():
        row = {"apartment": apt_name, "building": bld_name}
        for mode in modes:
            minutes = get_commute_time(apt_addr, bld_addr, mode)
            row[f"{mode}_min"] = minutes
            time.sleep(0.2)  # API 과호출 방지
        rows.append(row)
        print(f"✓ {apt_name} → {bld_name}")

df = pd.DataFrame(rows)
df.to_csv("commute_matrix.csv", index=False)
print("완료! commute_matrix.csv 저장됨")