"""
Analisi combinata risultati open water e piscina.

Questo script utilizza l'API World Aquatics per estrarre gli atleti che hanno
partecipato alle gare di maratona 10 km in acque libere in un determinato
campionato e confronta i loro risultati nelle gare in piscina sulla stessa
manifestazione (lunghezza vasca 50 m) sulle distanze 400, 800 e 1500 m.

Per utilizzare lo script, impostare la variabile `competition_id` con
l'identificatore del campionato (può essere ottenuto con lo script
``API_WorldAquatics_CompetitionsID.py``). Il programma scarica la lista
degli eventi, filtra quelli open water da 10 km e quelli di nuoto
freestyle nelle distanze indicate, quindi incrocia i partecipanti per
nome. In uscita viene generato un file Excel con i tempi di gara in
piscina per ogni atleta che ha nuotato i 10 km open water.

Nota: le richieste all'API potrebbero richiedere credenziali o token e
potrebbero essere soggette a limitazioni. Assicurarsi di avere accesso
appropriato prima di eseguire lo script.
"""

import os
import requests
import pandas as pd


# Impostazioni globali
# Inserire qui l'ID della competizione (ad esempio "4725" per Singapore 2025)
competition_id = "4725"

# Intestazioni HTTP simulate per l'API
headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
    "Origin": "https://www.worldaquatics.com",
    "Referer": "https://www.worldaquatics.com",
}


def fetch_events(comp_id: str) -> dict:
    """Fetch list of all events for a competition.

    Args:
        comp_id: Unique identifier of the competition.

    Returns:
        Parsed JSON response containing sports and disciplines.
    """
    url = f"https://api.worldaquatics.com/fina/competitions/{comp_id}/events"
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()


def fetch_event_results(event_id: str) -> list[dict]:
    """Fetch results for a specific event (final heat).

    Args:
        event_id: Unique identifier of the event.

    Returns:
        List of result dictionaries containing athlete data.
    """
    url = f"https://api.worldaquatics.com/fina/events/{event_id}"
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    data = resp.json()
    # Use first heat (final) results
    heat_results = data.get("Heats", [])[0].get("Results", [])
    return heat_results


def get_open_water_10km_ids(events_data: dict) -> list[str]:
    """Extract event IDs for 10 km open water races.

    Args:
        events_data: JSON data returned from fetch_events().

    Returns:
        List of event identifiers for 10 km open water disciplines.
    """
    ow_ids: list[str] = []
    for sport in events_data.get("Sports", []):
        # Open Water code is 'OW'
        if sport.get("Code") == "OW":
            for disc in sport.get("DisciplineList", []):
                name = disc.get("DisciplineName", "")
                # DisciplineName examples: 'Women 10km', 'Men 10km'
                if "10km" in name:
                    ow_ids.append(disc.get("Id"))
    return ow_ids


def get_pool_freestyle_ids(events_data: dict, distances: list[str] = None) -> list[tuple[str, str]]:
    """Extract event IDs for freestyle pool races of specified distances.

    Args:
        events_data: JSON data returned from fetch_events().
        distances: List of distance strings to match (e.g. ['400m','800m','1500m']).

    Returns:
        List of tuples (event_id, discipline_name).
    """
    if distances is None:
        distances = ["400m", "800m", "1500m"]
    pool_ids: list[tuple[str, str]] = []
    for sport in events_data.get("Sports", []):
        # Swimming code is 'SW'
        if sport.get("Code") == "SW":
            for disc in sport.get("DisciplineList", []):
                name = disc.get("DisciplineName", "")
                # Filter for freestyle long course distances
                if "Freestyle" in name and any(dist in name for dist in distances):
                    pool_ids.append((disc.get("Id"), name))
    return pool_ids


def athlete_key(first_name: str, last_name: str) -> tuple[str, str]:
    """Normalize athlete name into a key for matching.

    Args:
        first_name: First name of athlete.
        last_name: Last name of athlete.

    Returns:
        Tuple of normalized lower-case names.
    """
    return (first_name.strip().lower(), last_name.strip().lower())


def analyse_competition(comp_id: str) -> pd.DataFrame:
    """Perform the open water vs pool analysis for one competition.

    Args:
        comp_id: Competition identifier.

    Returns:
        DataFrame summarising pool results for athletes who raced 10 km OW.
    """
    events_data = fetch_events(comp_id)
    ow_ids = get_open_water_10km_ids(events_data)
    pool_ids = get_pool_freestyle_ids(events_data)

    # Collect athletes from open water 10 km events
    ow_athletes: dict[tuple[str, str], dict[str, any]] = {}
    for eid in ow_ids:
        results = fetch_event_results(eid)
        for res in results:
            fn = res.get("FirstName", "")
            ln = res.get("LastName", "")
            key = athlete_key(fn, ln)
            # Record participation; we could also store OW rank/time
            if key not in ow_athletes:
                ow_athletes[key] = {
                    "first_name": fn,
                    "last_name": ln,
                    "country": res.get("NAT", ""),
                }

    # Initialise dictionary for pool results per athlete
    athlete_pool_results: dict[tuple[str, str], dict[str, str]] = {}
    for eid, name in pool_ids:
        results = fetch_event_results(eid)
        for res in results:
            fn = res.get("FirstName", "")
            ln = res.get("LastName", "")
            key = athlete_key(fn, ln)
            # We are interested only in athletes who raced 10 km OW
            if key not in ow_athletes:
                continue
            # Compose event label, e.g. 'Men 400m Freestyle'
            event_label = name
            time = res.get("Time", "")
            # Use earliest (best) time if multiple entries
            athlete_pool_results.setdefault(key, {})
            if event_label not in athlete_pool_results[key]:
                athlete_pool_results[key][event_label] = time
            else:
                # Keep shorter time (lexicographic comparison works with HH:MM:SS)
                existing = athlete_pool_results[key][event_label]
                if time and (not existing or time < existing):
                    athlete_pool_results[key][event_label] = time

    # Build final dataframe
    rows: list[dict[str, any]] = []
    all_event_labels = [name for _, name in pool_ids]
    for key, info in ow_athletes.items():
        row: dict[str, any] = {
            "Athlete": f"{info['first_name']} {info['last_name']}",
            "Country": info['country'],
        }
        pool_data = athlete_pool_results.get(key, {})
        for label in all_event_labels:
            row[label] = pool_data.get(label, None)
        rows.append(row)

    df = pd.DataFrame(rows)
    # Remove duplicate columns if same event appears multiple times across genders
    df = df.loc[:, ~df.columns.duplicated()]
    return df


def main() -> None:
    """Main function to run analysis and save output."""
    print(f"Starting analysis for competition {competition_id}…")
    df = analyse_competition(competition_id)
    # Ensure output directory exists
    out_dir = "output_analysis"
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"ow_pool_{competition_id}.xlsx")
    df.to_excel(out_path, index=False)
    print(f"Analysis complete. Results saved to {out_path}")


if __name__ == "__main__":
    main()