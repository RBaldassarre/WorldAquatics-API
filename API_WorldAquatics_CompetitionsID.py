## Competition_id

import os
import requests
import pandas as pd

# === Multiple years ===
years = [2024, 2025] # [2024] or [2024, 2025]
all_comps = []

# === Settings ===
base_dir = os.path.dirname(os.path.abspath(__file__)) #Path File
output_dir = os.path.join(base_dir, "output_competitionsID")
os.makedirs(output_dir, exist_ok=True)


headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
    "Origin": "https://www.worldaquatics.com",
    "Referer": "https://www.worldaquatics.com"
}

for year in years:
    date_from = f"{year}-01-01T00:00:00+00:00"
    date_to   = f"{year + 1}-01-01T00:00:00+00:00"
    page = 0

    print(f"\n🔎 Fetching competitions for {year}...")

    while True:
        url = "https://api.worldaquatics.com/fina/competitions"
        params = {
            "pageSize": 100,
            "venueDateFrom": date_from,
            "venueDateTo": date_to,
            "disciplines": "",
            "group": "FINA",
            "sort": "dateFrom,asc",
            "page": page
        }

        res = requests.get(url, params=params, headers=headers)
        res.raise_for_status()
        data = res.json()

        comps = data.get("content", [])
        all_comps.extend(comps)

        if page >= data["pageInfo"]["numPages"] - 1:
            break
        page += 1

# === Create DataFrame
rows = []
for c in all_comps:
    row = {
        "id": c["id"],
        "name": c["name"],
        "city": c["location"]["city"],
        "country": c["location"]["countryName"],
        "disciplines": ", ".join(c.get("disciplines", [])),
        "date_from": c["dateFrom"][:10],
        "date_to": c["dateTo"][:10]
    }
    rows.append(row)

# === Save as Excel file
df = pd.DataFrame(rows)
year_str = "_".join(map(str, years))
excel_path = os.path.join(output_dir, f"competitions_{year_str}.xlsx")
df.to_excel(excel_path, index=False)

# === Print final preview
print(f"\n✅ Excel file saved: {excel_path}")
print(df.head())
