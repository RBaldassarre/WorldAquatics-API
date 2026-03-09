## API_WorldAquatics

import os
import requests
import pandas as pd

# === Settings ===
comp_ids = ["1", "2"]  # multiple competitions
disc_input = ["SW", "OW"]
fetch = True  # True = intersection (AND), False = union (OR)
gender = ""   # "M", "F", or "" for all
cty_input = ""  # e.g. "ITA", ["ITA", "USA"], or "" for all

# === Optional discipline filter ===
target_races = []

# === API setup ===
headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
    "Origin": "https://www.worldaquatics.com",
    "Referer": "https://www.worldaquatics.com"
}

# === Output ===
base_dir = os.path.dirname(os.path.abspath(__file__))
out_dir = os.path.join(base_dir, "output_athletes")
os.makedirs(out_dir, exist_ok=True)

# === Helpers ===
def normalize_input(val):
    return val if isinstance(val, list) else [val] if val else [""]

disc_list = normalize_input(disc_input)
cty_list = normalize_input(cty_input)

def fetch_data(disc, cty):
    print(f"📡 Downloading | discipline: {disc or 'ALL'} | gender: {gender or 'ALL'} | country: {cty or 'ALL'}")

    params = {
        "discipline": disc,
        "gender": gender,
        "countryId": cty
    }

    try:
        res = requests.get(url, headers=headers, params=params)
        res.raise_for_status()
        return res.json()

    except requests.exceptions.HTTPError as e:
        print(f"⚠️ competition not found: {url}")
        return []

def parse_athletes(data, filter_ids=None, ow_results=None, sw_results=None):
    rows = []
    for c in data:
        c_name = c.get("CountryName", "")
        for a in c.get("Participations", []):

            pid = a.get("PersonId")
            if filter_ids and pid not in filter_ids:
                continue

            fn = a.get("PreferredFirstName", "")
            ln = a.get("PreferredLastName", "")
            full_name = f"{fn} {ln}".strip()

            g_raw = a.get("Gender")
            g_str = "M" if g_raw == 0 else "F" if g_raw == 1 else ""

            dob_raw = a.get("DOB")
            dob = dob_raw[:10] if dob_raw else ""

            d_list = [d.get("DisciplineName", "") for d in a.get("Disciplines", [])]
            d_str = " / ".join(d_list)

            if target_races and not all(r in d_list for r in target_races):
                continue

            rows.append({
                "Country": c_name,
                "Athlete": full_name,
                "Gender": g_str,
                "DOB": dob,
                "Discipline": d_str,
                "OW_10km_place": ow_results.get(pid, "") if ow_results else "",
                "SW_result": sw_results.get(pid, "") if sw_results else ""
            })
    return rows

# === Main Logic ===
all_rows = []
for comp_id in comp_ids:

    url = f"https://api.worldaquatics.com/fina/competitions/{comp_id}/athletes"

    if fetch:

        data_dict = {}
        id_sets = []

        for disc in disc_list:
            data = []
            for cty in cty_list:
                data.extend(fetch_data(disc, cty))
            data_dict[disc] = data

            ids = {a["PersonId"] for c in data for a in c.get("Participations", [])}
            id_sets.append(ids)

        common_ids = set.intersection(*id_sets)

        for disc in disc_list:
            all_rows.extend(parse_athletes(data))

    else:

        for disc in disc_list:
            for cty in cty_list:
                data = fetch_data(disc, cty)
                all_rows.extend(parse_athletes(data))

# === Export ===
df = pd.DataFrame(all_rows)
# Group by athlete and merge disciplines
df = df.groupby(["Athlete", "DOB", "Gender", "Country"], as_index=False).agg({
    "Discipline": " / ".join,
    "OW_10km_place": "first",
    "SW_result": "first"
})

# Sort by Country
df = df.sort_values(by="Country")

suffix = "-".join(disc_list) if disc_list and any(disc_list) else "ALL"
if fetch:
    suffix += "_both"
if gender:
    suffix += f"_{gender}"
if cty_input:
    suffix += "_" + "-".join(cty_list)

out_file = os.path.join(out_dir, f"athletes_{suffix}.xlsx")
df.to_excel(out_file, index=False, engine="openpyxl")
print(f"✅ Saved {len(df)} athletes to: {out_file}")


