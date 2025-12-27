## Competition_id
import os
import sys
import requests
import pandas as pd

# Years input
def years_input(args):
    """Parse years from CLI."""
    arg = " ".join(args).lower().strip()

    if not arg:
        return list(range(2000, 2026))  # default 2000-2025

    if "to" in arg:
        start, end = arg.split("to")
        return list(range(int(start.strip()), int(end.strip()) + 1))

    if "," in arg:
        return [int(y.strip()) for y in arg.split(",")]

    return [int(arg)]

# Disciplines input
def disciplines_input(args):
    """
    Parse disciplines from CLI.
    DV, SW, SY, OW, WP, HD - ALL []
    Default: ['OW']
    """
    arg = " ".join(args).upper().strip()

    if not arg:
        return ["OW"]  # default

    if arg in ["ALL", "*"]:
        return []  # all disciplines

    if "," in arg:
        return [d.strip() for d in arg.split(",")]

    return [arg]

# Split years and disciplines
def split_years_and_disciplines(argv):
    years_tokens = []
    disc_tokens = []

    for tok in argv:
        t = tok.strip()
        low = t.lower()

        is_year = t.isdigit()
        is_to = low == "to"
        is_year_list = "," in t and all(p.strip().isdigit() for p in t.split(","))

        if disc_tokens:
            disc_tokens.append(t)
        elif is_year or is_to or is_year_list:
            years_tokens.append(t)
        else:
            disc_tokens.append(t)

    return years_tokens, disc_tokens


# Competition level filter (heuristic)
EXCLUDE_LEVEL_KEYWORDS = [
    "masters",
    "junior",
    "youth",
    "age group",
    "age-group",
    "u23",
    "u20",
    "u19",
    "u18",
    "u17",
    "u16",
    "u15",
    "u14",
]

def is_absolute_competition(comp):
    """Return True if competition looks like senior/elite (not masters/junior)."""
    name = comp.get("name") or ""

    comp_type_raw = comp.get("competitionType")

    if isinstance(comp_type_raw, dict):
        comp_type = (
            comp_type_raw.get("name")
            or comp_type_raw.get("code")
            or ""
        )
    else:
        comp_type = comp_type_raw or ""

    text = f"{name} {comp_type}".lower().strip()

    return not any(k in text for k in EXCLUDE_LEVEL_KEYWORDS)

def main():

    # Years and disciplines input
    years_args, disc_args = split_years_and_disciplines(sys.argv[1:])
    years = sorted(years_input(years_args))
    disciplines_filter = disciplines_input(disc_args)

    absolute_only = True  # True: exclude masters/junior/youth by keywords

    all_comps = []

    # Paths
    base_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(base_dir, "output_competitionsID")
    os.makedirs(output_dir, exist_ok=True)

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "Origin": "https://www.worldaquatics.com",
        "Referer": "https://www.worldaquatics.com",
    }

    # Fetch competitions
    for year in years:
        date_from = f"{year}-01-01T00:00:00+00:00"
        date_to = f"{year + 1}-01-01T00:00:00+00:00"
        page = 0

        print(f"\n🔎 Analyzing competitions for {year}...")

        while True:
            url = "https://api.worldaquatics.com/fina/competitions"
            params = {
                "pageSize": 100,
                "venueDateFrom": date_from,
                "venueDateTo": date_to,
                "disciplines": "",
                "group": "FINA",
                "sort": "dateFrom,asc",
                "page": page,
            }

            res = requests.get(url, params=params, headers=headers)
            res.raise_for_status()
            data = res.json()

            all_comps.extend(data.get("content", []))

            if page >= data["pageInfo"]["numPages"] - 1:
                break
            page += 1

    # Filter out masters/junior/youth competitions (best-effort)
    if absolute_only:
        all_comps = [c for c in all_comps if is_absolute_competition(c)]

    # Build rows
    rows = []
    for c in all_comps:
        location = c.get("location") or {}
        disciplines = c.get("disciplines", [])

        rows.append(
            {
                "id": c.get("id"),
                "name": c.get("name"),
                "city": location.get("city"),
                "country": location.get("countryName"),
                "disciplines": ", ".join(disciplines),
                "date_from": (c.get("dateFrom") or "")[:10],
                "date_to": (c.get("dateTo") or "")[:10],
            }
        )

    # Create DataFrame
    df = pd.DataFrame(rows)

    # Filter disciplines (OR)
    if disciplines_filter:
        pattern = "|".join(disciplines_filter)
        df = df[df["disciplines"].str.contains(pattern, na=False)]

    # Output name
    if len(years) > 3:
        year_str = f"{years[0]}_to_{years[-1]}"
    else:
        year_str = "_".join(map(str, years))

    excel_path = os.path.join(output_dir, f"competitions_{year_str}.xlsx")
    df.to_excel(excel_path, index=False)

    # Preview
    print(f"\n✅ Excel file saved: {excel_path}")
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 200)
    print(df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()