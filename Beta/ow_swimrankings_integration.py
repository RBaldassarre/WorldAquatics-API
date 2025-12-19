"""
Integration of open‑water results from the World Aquatics API with
personal best times from the SwimRankings library.

This script fetches the results for the 10 km open‑water event of a
specified competition and matches each athlete with their personal
best times over 400 m, 800 m and 1500 m freestyle (long‑course
pool, i.e. 50 m).  The personal bests are retrieved using the
``swimrankings`` Python library, which wraps the swimrankings.net
website.  Results are saved to an Excel file for further analysis.

Note
====
The ``swimrankings`` library is not available in this environment,
and swimrankings.net may block automated access.  Run this script
in an environment where ``swimrankings`` is installed and network
access to swimrankings.net is allowed.  When importing the library
fails, personal bests will not be populated but the script will
still fetch open‑water results.

Usage
-----
```
python ow_swimrankings_integration.py --competition_id 4725
```

Replace ``4725`` with the ID of the open‑water competition you
wish to analyse.  The script produces an Excel file named
``open_water_pool_analysis.xlsx`` in the current working directory.
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any

import requests
import pandas as pd

competition_id = "3328"

try:
    # Import the swimrankings library if available.  This import will
    # fail in restricted environments; in that case, personal bests
    # will not be fetched.
    from swimrankings import Athletes
    SWIMRANKINGS_AVAILABLE = True
except Exception:
    SWIMRANKINGS_AVAILABLE = False


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


WORLD_AQUATICS_BASE = "https://api.worldaquatics.com"


@dataclass
class AthleteResult:
    """Data structure to hold combined open‑water result and personal bests."""
    name: str
    nationality: str
    open_water_time: str
    personal_bests: Dict[str, Dict[str, Optional[str]]] = field(default_factory=dict)


def fetch_competition_events(competition_id: int) -> List[Dict[str, Any]]:
    """
    Retrieve all events for a given competition from the World Aquatics API.

    Parameters
    ----------
    competition_id : int
        Identifier of the competition (e.g., 4725 for the 2025 World
        Aquatics Championships).

    Returns
    -------
    List[Dict[str, Any]]
        A list of event metadata dictionaries.
    """
    url = f"{WORLD_AQUATICS_BASE}/competitions/{competition_id}/events"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        events = response.json()
        return events if isinstance(events, list) else []
    except Exception as e:
        logger.error("Failed to fetch competition events: %s", e)
        return []


def fetch_event_results(event_id: int) -> List[Dict[str, Any]]:
    """
    Retrieve results for a specific event from the World Aquatics API.

    Parameters
    ----------
    event_id : int
        Identifier of the event (within a competition).

    Returns
    -------
    List[Dict[str, Any]]
        A list of result dictionaries containing athlete name, nationality and
        performance time.  The exact keys may vary depending on the API.
    """
    url = f"{WORLD_AQUATICS_BASE}/eventResults/{event_id}"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        # Some events return results under 'Results', others directly as list
        if isinstance(data, dict) and 'Results' in data:
            return data['Results']
        elif isinstance(data, list):
            return data
        else:
            return []
    except Exception as e:
        logger.error("Failed to fetch event results: %s", e)
        return []


def get_swimrankings_personal_bests(name: str, country: str | None = None) -> Dict[str, Dict[str, Optional[str]]]:
    """
    Retrieve personal bests from swimrankings.net for 400, 800 and 1500 m
    freestyle (long course) for a given athlete.

    Parameters
    ----------
    name : str
        Full name of the athlete in "Last, First" format.
    country : str or None
        Optional country code to narrow down the search.

    Returns
    -------
    Dict[str, Dict[str, Optional[str]]]
        Mapping from event ("400 Free", "800 Free", "1500 Free") to a
        dictionary containing time, date, meet and location.  Missing
        information is represented by None.  If the swimrankings
        library is unavailable or no matches are found, an empty
        dictionary is returned.
    """
    target_events = {"400 Free", "800 Free", "1500 Free"}
    results: Dict[str, Dict[str, Optional[str]]] = {}

    if not SWIMRANKINGS_AVAILABLE:
        logger.warning("swimrankings library not available; personal bests cannot be fetched")
        return results

    # Extract last and first names for search.  The library expects
    # separate names when filtering by country.
    if ", " in name:
        last_name, first_name = name.split(", ", 1)
    else:
        parts = name.split()
        last_name = parts[0] if parts else name
        first_name = "".join(parts[1:]) if len(parts) > 1 else ""

    # Search for athletes by last name; filter by country if provided.
    athletes = Athletes(name=last_name)
    matches = []
    for athlete in athletes:
        if country and athlete.country != country:
            continue
        # Compare first names loosely (case insensitive)
        if first_name and athlete.first_name.lower() != first_name.lower():
            continue
        matches.append(athlete)

    if not matches:
        logger.warning("No matching athlete found in swimrankings for %s", name)
        return results

    # Assume the first match is the correct athlete.
    athlete = matches[0]
    try:
        details = athlete.get_details()
    except Exception as e:
        logger.error("Failed to fetch details for %s: %s", athlete.full_name, e)
        return results

    for pb in details.personal_bests:
        if pb.event in target_events and pb.course == "LCM":
            results[pb.event] = {
                "time": pb.time,
                "date": pb.date,
                "meet": pb.meet,
                "location": pb.location,
            }
    return results


def analyse_open_water_pool(competition_id: int) -> pd.DataFrame:
    """
    Perform the full analysis: fetch open‑water 10 km results and match them
    with personal bests in pool events.

    Parameters
    ----------
    competition_id : int
        ID of the competition containing the open‑water 10 km event.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing athlete names, nationalities, open‑water
        times and personal bests for 400, 800 and 1500 m freestyle.
    """
    # Fetch all events in the competition
    events = fetch_competition_events(competition_id)
    logger.info("Fetched %d events for competition %d", len(events), competition_id)

    # Identify the 10 km open‑water event.  Event names often contain
    # "10km" or similar; adapt this filter if necessary.
    ow_event_id = None
    for evt in events:
        name = evt.get("Name", "").lower()
        discipline = evt.get("Discipline", "").lower()
        if "10" in name and "open water" in discipline:
            ow_event_id = evt.get("Id")
            break
    if ow_event_id is None:
        logger.error("Could not find the 10 km open‑water event in competition %d", competition_id)
        return pd.DataFrame()

    # Fetch results for the open‑water event
    results = fetch_event_results(ow_event_id)
    logger.info("Fetched %d open‑water results", len(results))

    # Build a list of AthleteResult objects
    athlete_results: List[AthleteResult] = []
    for res in results:
        name = res.get("AthleteName") or res.get("Name")
        nationality = res.get("Nation") or res.get("CountryCode") or res.get("NAT")
        time = res.get("Result") or res.get("Time") or res.get("Rank" )
        if not name or not time:
            continue
        pb = get_swimrankings_personal_bests(name, country=nationality)
        athlete_results.append(AthleteResult(name=name, nationality=nationality, open_water_time=time, personal_bests=pb))

    # Construct DataFrame
    rows = []
    for ar in athlete_results:
        # Ensure keys for events exist
        for event in ["400 Free", "800 Free", "1500 Free"]:
            pb_info = ar.personal_bests.get(event, {})
            rows.append({
                "Athlete": ar.name,
                "Nation": ar.nationality,
                "OpenWaterTime": ar.open_water_time,
                "PoolEvent": event,
                "PB_Time": pb_info.get("time"),
                "PB_Date": pb_info.get("date"),
                "PB_Meet": pb_info.get("meet"),
                "PB_Location": pb_info.get("location"),
            })
    df = pd.DataFrame(rows)
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyse open‑water and pool performance")
    parser.add_argument("--competition_id", type=int, required=True, help="Competition ID for the 10 km open‑water event")
    parser.add_argument("--output", type=str, default="open_water_pool_analysis.xlsx", help="Output Excel file")
    args = parser.parse_args()

    df = analyse_open_water_pool(args.competition_id)
    if df.empty:
        logger.error("No data to save")
        return
    df.to_excel(args.output, index=False)
    logger.info("Analysis complete. Results saved to %s", args.output)


if __name__ == "__main__":
    df = analyse_open_water_pool(int(competition_id))
    if not df.empty:
        df.to_excel("open_water_pool_analysis.xlsx", index=False)
        logger.info("File salvato")
