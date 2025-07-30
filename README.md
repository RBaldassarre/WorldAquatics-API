# World Aquatics API - Athletes & OW Results

This repository contains two Python scripts for retrieving athletes' data and Open Water (OW) competition results from the World Aquatics API.

---

## Scripts

### `API_WorldAquatics.py`
Fetches athlete data by:
- Discipline (`SW`, `OW`, etc.)
- Gender (`M`, `F`, or all)
- Country (`ITA`, `USA`, etc.)
- Optionally: Only athletes participating in *all* selected disciplines

🛠 Configurable options inside the script:
```python
disc_input = ["SW", "OW"]      # disciplines to include
fetch = True                   # True = AND / False = OR
gender = "M"                   # "M", "F", or ""
cty_input = ["ITA", "USA"]     # list of country codes or "" for all
