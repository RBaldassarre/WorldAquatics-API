## API_WorldAquatics

import os
import requests
import pandas as pd

# === Settings ===
comp_ids = ["468", "416", "312", "262", "213", "95", "5", "2902", "1", "2943", "4725"] # List of competition IDs to fetch
disc_input = ["SW", "OW"]
fetch = True  # True = intersection (AND), False = union (OR)
gender = ""   # "M", "F", or "" for all
cty_input = ""  # e.g. "ITA", ["ITA", "USA"], or "" for all

# === Optional discipline filter ===
target_races = []

# === Ramking Results ===
SW_RESULTS_ENABLED = True  # Set to False to skip fetching SW event results (faster)
OW_RESULTS_ENABLED = True  # Set to False to skip fetching 10km OW rankings (faster)

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
    res = requests.get(url, headers=headers, params=params)
    res.raise_for_status()
    return res.json()

def get_10km_ranking(comp_id):

    events_url = f"https://api.worldaquatics.com/fina/competitions/{comp_id}/events"
    res = requests.get(events_url, headers=headers)
    res.raise_for_status()
    events_data = res.json()

    event_ids = []

    # search 10km OW events
    for sport in events_data.get("Sports", []):
        if sport.get("Code") == "OW":
            for d in sport.get("DisciplineList", []):
                name = d.get("DisciplineName", "").lower()
                if "10km" in name:
                    event_ids.append(d.get("Id"))

    ranking = {}

    for event_id in event_ids:

        event_url = f"https://api.worldaquatics.com/fina/events/{event_id}"
        res_event = requests.get(event_url, headers=headers)
        res_event.raise_for_status()
        event_data = res_event.json()

        results = event_data["Heats"][0]["Results"]

        for athlete in results:
            pid = athlete.get("PersonId")
            rank = athlete.get("Rank")

            if pid:
                ranking[pid] = rank

    return ranking

def get_sw_results(comp_id):

    events_url = f"https://api.worldaquatics.com/fina/competitions/{comp_id}/events"
    res = requests.get(events_url, headers=headers)
    res.raise_for_status()
    events_data = res.json()

    results_dict = {}

    for sport in events_data.get("Sports", []):

        if sport.get("Code") != "SW":
            continue

        for d in sport.get("DisciplineList", []):

            event_id = d.get("Id")
            event_name = d.get("DisciplineName").replace(" ", "_").replace("/", "_")

            event_url = f"https://api.worldaquatics.com/fina/events/{event_id}"

            try:

                res_event = requests.get(event_url, headers=headers)
                res_event.raise_for_status()
                event_data = res_event.json()

                heats = event_data.get("Heats", [])

                for h in heats:

                    phase = str(h.get("PhaseName", "")).upper()
                    if phase == "SUMMARY":
                        continue
                    # print(f"   Processing SW event: {event_name} | phase: {phase}")
                    heat_results = h.get("Results", [])

                    for athlete in heat_results:

                        pid = athlete.get("PersonId")
                        rank = athlete.get("FinalRank") or athlete.get("Rank")

                        if not pid:
                            continue

                        if pid not in results_dict:
                            results_dict[pid] = {}

                        col_name = f"{event_name}_{phase}_Rk"

                        if col_name not in results_dict[pid]:
                            results_dict[pid][col_name] = rank

            except Exception as e:

                print(f"⚠️ Skipping SW event {event_name} → {e}")
                continue

    return results_dict

def parse_athletes(data, filter_ids=None, ow_rank=None, sw_results=None):

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

            row = {
                "Competition_Id": comp_id,
                "Country": c_name,
                "Athlete": full_name,
                "Gender": g_str,
                "DOB": dob,
                "Discipline": d_str,
                "10km_Rk": ow_rank.get(pid, "") if ow_rank else ""
            }

            # add SW results
            if sw_results and pid in sw_results:
                row.update(sw_results[pid])

            rows.append(row)

    return rows

# ========== Main ==========
all_dfs = []
for comp_id in comp_ids:
    try:
        print(f"\n🏊 Processing competition {comp_id}")
        url = f"https://api.worldaquatics.com/fina/competitions/{comp_id}/athletes"

        ow_rank = {}
        if OW_RESULTS_ENABLED:
            try:
                ow_rank = get_10km_ranking(comp_id)
            except Exception:
                pass

        sw_results = {}
        if SW_RESULTS_ENABLED:
            try:
                sw_results = get_sw_results(comp_id)
            except Exception:
                pass

        # === Main Logic ===
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

            rows = []
            for disc in disc_list:
                rows.extend(parse_athletes(
                    data_dict[disc],
                    filter_ids=common_ids,
                    ow_rank=ow_rank,
                    sw_results=sw_results
                ))

        else:
            all_data = []
            for disc in disc_list:
                for cty in cty_list:
                    all_data.extend(fetch_data(disc, cty))

            rows = parse_athletes(all_data, ow_rank=ow_rank)

        if not rows:
            continue

        df = pd.DataFrame(rows)

        group_cols = ["Competition_Id", "Gender", "Athlete", "DOB", "Country"]
        agg_dict = {}

        for col in df.columns:
            if col == "Discipline":
                agg_dict[col] = " / ".join
            elif col == "10km_Rk":
                agg_dict[col] = lambda x: next((v for v in x if v != ""), "")
            elif col not in group_cols:
                agg_dict[col] = "first"

        df = df.groupby(group_cols, as_index=False).agg(agg_dict)
        df = df.replace("", pd.NA)
        df = df.dropna(axis=1, how="all") # remove empty columns
        all_dfs.append(df) # save dataframe in list
        
    except Exception as e:
        print(f"❌ Skipping competition {comp_id} → {e}")
        continue

# === Export ===
final_df = pd.concat(all_dfs, ignore_index=True)
suffix = "-".join(disc_list) if disc_list and any(disc_list) else "ALL"
if fetch:
    suffix += "_both"
comp_str = "-".join(comp_ids)
out_file = os.path.join(out_dir, f"athletes_{comp_str}_{suffix}.xlsx")
final_df.to_excel(out_file, index=False, engine="openpyxl")
print(f"✅ Saved {len(final_df)} athletes to: {out_file}")

