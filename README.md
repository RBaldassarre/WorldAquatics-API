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

Analyzes World Aquatics competitions for one or more years.
The script retrieves all official competitions and extracts:

- Competition ID
- Name, city, country
- Disciplines involved (`SW`, `OW`, `WP`, etc.)
- Start and end dates

Results are exported to an Excel file and saved in the
 `output_competitionsID/` folder.

**Usage from terminal**
You can specify the years directly from the command line:

```python
python API_WorldAquatics_CompetitionsID.py 2022
python API_WorldAquatics_CompetitionsID.py 2022,2023
python API_WorldAquatics_CompetitionsID.py 2020 to 2023
```

If no years are provided, the script analyzes **all competitions from 2000 to 2025** by default.

**Configurable options inside the script**

```
disciplines_filter = ["OW"]  # SW, OW, DV, WT, HY or [] for all
```

The output Excel file is automatically named according to the selected year range(e.g. `competitions_2020_to_2023.xlsx`).

**Discipline filter (optional)**
By default, the script analyzes **Open Water (OW)** competitions only.
You can change the default behavior inside the script or override it from the command line by specifying one or more discipline codes (`SW`, `OW`, `DV`, `WT`, `HY`).

```
disciplines_filter = ["OW"]  # [] means all disciplines
```

Examples:

```
python API_WorldAquatics_CompetitionsID.py 2022 OW
python API_WorldAquatics_CompetitionsID.py 2022 OW,SW
python API_WorldAquatics_CompetitionsID.py 2022 ALL

python API_WorldAquatics_CompetitionsID.py 2022 to 2025 OW
python API_WorldAquatics_CompetitionsID.py 2022 to 2025 ALL
```

**Level competitions filter (optional)**
World Aquatics does not provide a dedicated field to explicitly distinguish **Senior / Absolute** competitions from **Junior, Youth, or Masters** events.
For this reason, the script applies a **best-effort heuristic filter** based on competition metadata.
When enabled, competitions whose name or type contains keywords such as **Masters**, **Junior**, **Youth**, **U18**, **U20**, **Age Group**, etc. are automatically excluded.
This allows the analysis to focus on **senior / elite competitions only**.
The filter can be enabled or disabled directly in the script:

```python
absolute_only = True  # exclude masters/junior/youth competitions
```

### `API_WorldAquatics.py`

Fetches athlete data by:to 2025 OW

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
