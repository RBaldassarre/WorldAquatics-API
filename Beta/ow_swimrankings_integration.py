"""
OW 10km -> PB pool (400/800/1500 LCM) via SwimRankings

- Fetch OW event list for a competition (FINA endpoint)
- Auto-pick 10km events (Men/Women) OR let user choose
- Fetch event results
- For each athlete, (optional) fetch PBs from swimrankings (LCM) for:
  400 Free, 800 Free, 1500 Free
- Save to Excel

Python: 3.10+
"""

from __future__ import annotations

import os
import time
import logging
import unicodedata
import re
from typing import Any, Dict, List, Optional, Tuple

import requests
import pandas as pd

# =======================
# SETTINGS (EDIT HERE)
# =======================
COMPETITION_ID = "4725"              # e.g. 4725 = Singapore 2025
AUTO_PICK_10KM = True               # True = auto pick "10km"; False = prompt choose
GENDER_FILTER = ""                  # "M" / "W" / "" (all)
OUTPUT_XLSX = "ow_pool_pb.xlsx"     # output file name (saved next to this .py)
SLEEP_BETWEEN_ATHLETES = 0.2        # be nice to swimrankings
DEBUG_PRINT_OW_EVENTS = False       # True to print all OW events found
# =======================

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
lg = logging.getLogger("ow-pb")

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
    "Origin": "https://www.worldaquatics.com",
    "Referer": "https://www.worldaquatics.com",
}

FINA_BASE = "https://api.worldaquatics.com/fina"

try:
    from swimrankings import Athletes
    SWIMRANKINGS_AVAILABLE = True
except Exception:
    SWIMRANKINGS_AVAILABLE = False


# -----------------------
# Helpers
# -----------------------
def http_get_json(url: str, retries: int = 3, pause: float = 0.8) -> Any:
    """GET JSON with headers + small retry."""
    last_err = None
    for _ in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            time.sleep(pause)
    raise RuntimeError(f"GET failed: {url} -> {last_err}")


def norm_name(s: str) -> str:
    """
    Normalize names for SwimRankings search:
    - remove accents
    - keep letters/spaces/hyphen/apostrophe
    - collapse spaces
    """
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^A-Za-z\s\-']", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def split_last_first(full_name: str) -> Tuple[str, str]:
    """Return (last_name, first_name) from 'Last, First' or 'Last First'."""
    full_name = full_name.strip()
    if ", " in full_name:
        last_name, first_name = full_name.split(", ", 1)
        return last_name.strip(), first_name.strip()

    parts = full_name.split()
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])


# -----------------------
# World Aquatics (OW)
# -----------------------
def fetch_ow_events(competition_id: str) -> List[Dict[str, Any]]:
    """Return OW DisciplineList items: {name, gender, id}."""
    url = f"{FINA_BASE}/competitions/{competition_id}/events"
    data = http_get_json(url)

    lg.info("Competition name: %s", data.get("Name"))

    ow_events: List[Dict[str, Any]] = []
    for sport in data.get("Sports", []):
        if sport.get("Code") != "OW":
            continue
        for d in sport.get("DisciplineList", []):
            ow_events.append({
                "name": d.get("DisciplineName", ""),
                "gender": d.get("Gender", ""),
                "id": d.get("Id", ""),
            })
    return ow_events


def pick_events(ow_events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Pick events automatically (10km) or prompt user."""
    if not ow_events:
        return []

    filtered = [
        e for e in ow_events
        if (not GENDER_FILTER or (e.get("gender") or "").upper() == GENDER_FILTER.upper())
    ]

    if DEBUG_PRINT_OW_EVENTS:
        for i, e in enumerate(filtered, 1):
            lg.info("OW #%d: %s (%s) -> %s", i, e["name"], e["gender"], e["id"])

    if AUTO_PICK_10KM:
        picked: List[Dict[str, Any]] = []
        for e in filtered:
            nm = (e.get("name") or "").lower()
            if "10" in nm and "km" in nm:
                picked.append(e)
        return picked if picked else filtered

    print("\n📡 Open Water events:")
    for i, e in enumerate(filtered, start=1):
        print(f"{i}. {e['name']} ({e['gender']}) -> {e['id']}")
    idx = int(input("\n👉 Enter event number: ")) - 1
    if idx < 0 or idx >= len(filtered):
        raise ValueError("Invalid selection")
    return [filtered[idx]]


def fetch_event_results(event_id: str) -> List[Dict[str, Any]]:
    """
    Return list of athletes:
    {wa_id, full_name, nat, time, rank}
    """
    url = f"{FINA_BASE}/events/{event_id}"
    data = http_get_json(url)

    heats = data.get("Heats", [])
    if not heats:
        return []

    results = heats[0].get("Results", [])
    out: List[Dict[str, Any]] = []

    for a in results:
        wa_id = (
            a.get("PersonId")
            or a.get("AthleteId")
            or a.get("CompetitorId")
            or a.get("Id")
            or ""
        )

        fn = (a.get("FirstName") or "").strip()
        ln = (a.get("LastName") or "").strip()
        full_name = f"{ln}, {fn}".strip(", ").strip()

        out.append({
            "wa_id": wa_id,
            "full_name": full_name,
            "nat": a.get("NAT", ""),
            "time": a.get("Time", ""),
            "rank": a.get("Rank", ""),
        })

    return out


# -----------------------
# SwimRankings (PB)
# -----------------------
_PB_CACHE: Dict[Tuple[str, str], Dict[str, Dict[str, Optional[str]]]] = {}


def swimrankings_pbs(full_name: str, nat: str) -> Dict[str, Dict[str, Optional[str]]]:
    """
    Return PBs for LCM: 400/800/1500 Free.
    - NEVER crash on AthleteNotFoundError
    - Normalize names
    - Try with NAT filter; if no match, retry without NAT
    """
    target = {"400 Free", "800 Free", "1500 Free"}
    pbs: Dict[str, Dict[str, Optional[str]]] = {}

    if not SWIMRANKINGS_AVAILABLE:
        return pbs

    cache_key = (full_name, nat)
    if cache_key in _PB_CACHE:
        return _PB_CACHE[cache_key]

    last_name_raw, first_name_raw = split_last_first(full_name)
    last_name = norm_name(last_name_raw).upper()
    first_name = norm_name(first_name_raw)

    if not last_name:
        _PB_CACHE[cache_key] = pbs
        return pbs

    def _search(country_filter: Optional[str]) -> Dict[str, Dict[str, Optional[str]]]:
        try:
            athletes = Athletes(name=last_name)
        except Exception as e:
            lg.warning("SwimRankings: no athletes for last_name='%s' (%s)", last_name, e)
            return {}

        matches = []
        for ath in athletes:
            try:
                ath_country = getattr(ath, "country", "")
                ath_first = getattr(ath, "first_name", "")
            except Exception:
                continue

            if country_filter and ath_country != country_filter:
                continue

            if first_name and ath_first.lower() != first_name.lower():
                continue

            matches.append(ath)

        if not matches:
            return {}

        ath = matches[0]
        try:
            details = ath.get_details()
        except Exception as e:
            lg.warning("SwimRankings: failed get_details for '%s' (%s)", getattr(ath, "full_name", full_name), e)
            return {}

        out_pbs: Dict[str, Dict[str, Optional[str]]] = {}
        try:
            for pb in details.personal_bests:
                if pb.event in target and pb.course == "LCM":
                    out_pbs[pb.event] = {
                        "time": pb.time,
                        "date": pb.date,
                        "meet": pb.meet,
                        "location": pb.location,
                    }
        except Exception as e:
            lg.warning("SwimRankings: PB parse error for '%s' (%s)", full_name, e)
            return {}

        return out_pbs

    pbs = _search(nat)
    if not pbs:
        pbs = _search(None)

    _PB_CACHE[cache_key] = pbs
    return pbs


# -----------------------
# Main
# -----------------------
def main() -> None:
    lg.info("Competition: %s", COMPETITION_ID)

    ow_events = fetch_ow_events(COMPETITION_ID)
    lg.info("OW events found: %d", len(ow_events))

    if not ow_events:
        lg.error("No OW events returned. Check COMPETITION_ID or API availability.")
        return

    selected = pick_events(ow_events)
    lg.info("Selected events: %s", [e["name"] for e in selected])

    rows: List[Dict[str, Any]] = []

    for ev in selected:
        ev_name = ev["name"]
        ev_id = ev["id"]

        lg.info("Downloading results: %s (%s)", ev_name, ev_id)
        athletes = fetch_event_results(ev_id)
        lg.info("Athletes: %d", len(athletes))

        for a in athletes:
            wa_id = a.get("wa_id", "")
            full_name = a["full_name"]
            nat = a["nat"]
            ow_time = a["time"]
            rank = a["rank"]

            pbs = swimrankings_pbs(full_name, nat)
            time.sleep(SLEEP_BETWEEN_ATHLETES)

            for pool_event in ["400 Free", "800 Free", "1500 Free"]:
                pb = pbs.get(pool_event, {})
                rows.append({
                    "OW_Event": ev_name,
                    "WA_ID": wa_id,
                    "Athlete": full_name,
                    "NAT": nat,
                    "OW_Rank": rank,
                    "OW_Time": ow_time,
                    "PoolEvent": pool_event,
                    "PB_Time": pb.get("time"),
                    "PB_Date": pb.get("date"),
                    "PB_Meet": pb.get("meet"),
                    "PB_Location": pb.get("location"),
                })

    df = pd.DataFrame(rows)
    if df.empty:
        lg.error("No data to save.")
        return

    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), OUTPUT_XLSX)
    df.to_excel(out_path, index=False)
    lg.info("Saved: %s", out_path)


if __name__ == "__main__":
    main()
