## API_WorldAquatics_OW

import os
import requests
import pandas as pd

# === Settings ===
competition_id = "4725"  # World Aquatics Championships - Singapore 2025
base_dir = os.path.dirname(os.path.abspath(__file__)) #Path File
output_dir = os.path.join(base_dir, "output_ow")
os.makedirs(output_dir, exist_ok=True)

event = "2025_WC_Singapore"

headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
    "Origin": "https://www.worldaquatics.com",
    "Referer": "https://www.worldaquatics.com"
}

# === Fetch Open Water events ===
print("\n📡 Downloading Open Water event list...")

events_url = f"https://api.worldaquatics.com/fina/competitions/{competition_id}/events"
res_events = requests.get(events_url, headers=headers)
res_events.raise_for_status()
events_data = res_events.json()

# === Filter OW disciplines
open_water_events = []
for sport in events_data.get("Sports", []):
    if sport.get("Code") == "OW":  # Open Water only
        for d in sport.get("DisciplineList", []):
            discipline_name = d.get("DisciplineName")
            discipline_id = d.get("Id")
            gender = d.get("Gender")
            print(f"{len(open_water_events) + 1}. {discipline_name} ({gender}) → ID: {discipline_id}")
            open_water_events.append({
                "name": discipline_name,
                "gender": gender,
                "id": discipline_id
            })

# === User selects event
selected_index = int(input("\n👉 Enter the number of the event to download: ")) - 1
selected_event = open_water_events[selected_index]
event_id = selected_event["id"]
event_name_safe = selected_event["name"].replace(" ", "_").replace("/", "_")

# === Fetch event results
event_url = f"https://api.worldaquatics.com/fina/events/{event_id}"
res_event = requests.get(event_url, headers=headers)
res_event.raise_for_status()
event_data = res_event.json()

# === Parse final heat results
heat_results = event_data["Heats"][0]["Results"]
discipline_name = event_data["DisciplineName"].replace(" ", "_")

# === Extract athlete results
rows = []
for athlete in heat_results:
    row = {
        "first_name": athlete["FirstName"],
        "last_name": athlete["LastName"],
        "country": athlete["NAT"],
        "bib": athlete.get("Bib", ""),
        "rank": athlete.get("Rank", ""),
        "final_time": athlete.get("Time", ""),
        "medal": athlete.get("MedalTag", "")
    }
    for i, s in enumerate(athlete.get("Splits", [])):
        row[f"split_{i + 1}"] = s.get("Time", "")
    rows.append(row)

# === Save as Excel file
df = pd.DataFrame(rows)
excel_path = os.path.join(output_dir, f"{event}_{discipline_name}.xlsx")
df.to_excel(excel_path, index=False)

print(f"\n✅ Excel file saved: {excel_path}")
print(df.head())
