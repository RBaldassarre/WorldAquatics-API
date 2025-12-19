## How to Use These Scripts – Step by Step

These three scripts are designed to work together for analyzing World Aquatics competitions:

---

### 1. `API_WorldAquatics_CompetitionsID.py`

**Goal**: Find all competitions (World Championships, World Cups, etc.) by year.
Use this to **retrieve the `competition_id`** you need for later.

Output: Excel file with competition names, cities, disciplines, and IDs
Example: Get ID = `4725` for "World Aquatics Championships - Singapore 2025"

---

### 2. `API_WorldAquatics.py`

**Goal**: Get a list of **athletes** by country, gender, and discipline.
Useful to know **who is competing** and in which discipline.

You can filter by `SW`, `OW`, etc., and even get athletes who are in **both** disciplines (e.g. SW *and* OW).

---

### 3. `API_WorldAquatics_OW.py`

**Goal**: Download **Open Water race results** (with split times) for a selected `competition_id`.
Extracts detailed results: athlete name, bib, country, final time, medal, and all **splits**.

Interactive: lets you choose the OW event you wantYou can **modify or extend** the script to:

- Download all OW races automatically
- Filter by gender or event name
- Export additional metrics

---

**Typical workflow**:

1. Use `API_WorldAquatics_CompetitionsID.py` → to find competition IDs
2. Use `API_WorldAquatics.py` → to explore athletes competing
3. Use `API_WorldAquatics_OW.py` → to download the OW race results

## Scripts

### `API_WorldAquatics_CompetitionsID.py`

Fetches all World Aquatics competitions for one or more years.

Returns:

- Competition ID
- Name, city, country
- Disciplines involved (`SW`, `OW`, `WP`, etc.)
- Start and end dates

Results are saved as an Excel file in the `output_competitionID/` folder.

Configurable options inside the script:

```python
years = [2024, 2025]   # or [2024] or [2024, 2025, 2026]
```

### `API_WorldAquatics.py`

Fetches athlete data by:

- Discipline (`SW`, `OW`, etc.)
- Gender (`M`, `F`, or all)
- Country (`ITA`, `USA`, etc.)
- Optionally: Only athletes participating in *all* selected disciplines

Configurable options inside the script:

```python
disc_input = ["SW", "OW"]       # disciplines to include
fetch = True                    # True = AND / False = OR
gender = "M"                    # "M", "F", or ""
cty_input = ["ITA", "USA"]      # list of country codes or "" for all
```

### `API_WorldAquatics_OW.py`

Downloads Open Water (OW) race results (including split times) for a selected event in a competition.

The script:

- Fetches all OW events from a given competition
- Prompts the user to **select a race (by distance/gender)**
- Downloads the full result list with split times
- Saves everything to Excel in the `output_ow/` folder

It extracts:

- Athlete name, country, bib, rank, final time, medal
- All intermediate split times (e.g., 1000m, 2000m, ...)

Configurable option inside the script:

```python
competition_id = "4725" # e.g. World Aquatics Championships - Singapore 2025
```

## License

This project is licensed under the Creative Commons Attribution-NonCommercial 4.0 International License.
You are free to use and modify it for non-commercial purposes.

🔗 [Read full license](https://creativecommons.org/licenses/by-nc/4.0/)
