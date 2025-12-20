# API_WorldAquatics_OW_Pool_Results_Integration_Beta
# Integrate World Aquatics Open Water results with Pool results from SwimRankings.net
# TODO: sync con API_Comeptitions 
# Gestione Temporale PB Y SB
# schema di incremento % diviso per ogni competizione
# saltare atleti che non completano la gara (salta in assenza di OW_Rank)

from __future__ import annotations

import os
import re
import time
import logging
import unicodedata
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

import requests
import pandas as pd
from bs4 import BeautifulSoup

# =======================
# SETTINGS
# =======================
COMPETITION_ID = "4725"
AUTO_PICK_10KM = True
OUTPUT_XLSX = "ow_pool_join.xlsx"

SLEEP_BETWEEN_ATHLETES = 0.2
SLEEP_BETWEEN_REQUESTS = 0.2

# target pool events (SwimRankings naming)
POOL_EVENTS = ["400 Free", "800 Free", "1500 Free"]

# =======================
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
lg = logging.getLogger("ow-pool-join")

HEADERS_WA = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
    "Origin": "https://www.worldaquatics.com",
    "Referer": "https://www.worldaquatics.com",
}

HEADERS_SR = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.swimrankings.net",
}

FINA_BASE = "https://api.worldaquatics.com/fina"
SWIMRANKINGS_ATHLETE_URL = "https://www.swimrankings.net/index.php?page=athleteDetail&athleteId={athlete_id}"

try:
    from swimrankings import Athletes
    SWIMRANKINGS_LIB = True
except Exception:
    SWIMRANKINGS_LIB = False


# =======================
# Helpers
# =======================
def http_get_json(url: str, headers: dict, retries: int = 3, pause: float = 0.8) -> Any:
    last_err = None
    for _ in range(retries):
        try:
            r = requests.get(url, headers=headers, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            time.sleep(pause)
    raise RuntimeError(f"GET failed: {url} -> {last_err}")


def http_get_text(url: str, headers: dict, retries: int = 3, pause: float = 0.8) -> str:
    last_err = None
    for _ in range(retries):
        try:
            r = requests.get(url, headers=headers, timeout=30)
            r.raise_for_status()
            return r.text
        except Exception as e:
            last_err = e
            time.sleep(pause)
    raise RuntimeError(f"GET failed: {url} -> {last_err}")


def norm_name(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^A-Za-z\s\-']", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def split_last_first(full_name: str) -> Tuple[str, str]:
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


def parse_iso_date(s: str) -> Optional[date]:
    if not s:
        return None
    # WA often uses "YYYY-MM-DDTHH:MM:SS"
    try:
        return datetime.fromisoformat(s.replace("Z", "")).date()
    except Exception:
        pass
    # try "YYYY-MM-DD"
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except Exception:
        return None


# =======================
# WA pool helpers/mapping for freestyle pool results
# =======================
def wa_time_to_seconds(t: str) -> Optional[float]:
    """Convert WA time strings like '3:55.12' or '15:01.34' to seconds."""
    if not t:
        return None
    t = str(t).strip()
    # Sometimes WA can return placeholders like '' or None
    if not t or t in {"-", "--"}:
        return None
    try:
        if ":" in t:
            mm, rest = t.split(":", 1)
            return float(mm) * 60.0 + float(rest)
        return float(t)
    except Exception:
        return None


def wa_guess_event_key(label: str) -> Optional[str]:
    """Map a discipline/event label to one of POOL_EVENTS (freestyle only)."""
    if not label:
        return None
    s = str(label).lower()
    # freestyle hints (English + common abbreviations)
    if not ("free" in s or "freestyle" in s or "fr" in s):
        return None
    if "400" in s:
        return "400 Free"
    if "800" in s:
        return "800 Free"
    if "1500" in s:
        return "1500 Free"
    return None


def wa_is_lcm_50m(d: Dict[str, Any]) -> bool:
    """Best-effort check for 50m pool / LCM in a result dict."""
    # direct numeric hints
    for k in ("PoolLength", "Pool", "PoolSize", "PoolConfiguration", "Course", "CourseCode"):
        v = d.get(k)
        if v is None:
            continue
        s = str(v).lower()
        if s in {"lcm", "50", "50m", "50 m", "50-m"}:
            return True
        if "50" in s and "25" not in s:
            return True
        if "lcm" in s or "long" in s:
            return True
        if "25" in s or "scm" in s:
            return False
    # no evidence -> assume unknown; treat as acceptable (many WA feeds omit course)
    return True


def wa_extract_pool_rows(js: Any) -> List[Dict[str, Any]]:
    """Recursively scan an unknown WA JSON structure and extract pool result-like rows."""
    rows: List[Dict[str, Any]] = []

    def visit(x: Any) -> None:
        if isinstance(x, dict):
            # Candidate row if it has a time-like field and some label/date field
            time_val = x.get("Time") or x.get("Result") or x.get("SwimTime") or x.get("Performance")
            date_val = x.get("Date") or x.get("StartDate") or x.get("CompetitionDate") or x.get("From")
            label_val = x.get("DisciplineName") or x.get("EventName") or x.get("Name") or x.get("Event") or x.get("Discipline")

            if time_val and (date_val or label_val):
                label = str(label_val) if label_val is not None else ""
                ev_key = wa_guess_event_key(label)

                # Try secondary label sources (nested)
                if not ev_key:
                    for k2 in ("Discipline", "Event", "Race", "Competition"):
                        v2 = x.get(k2)
                        if isinstance(v2, dict):
                            lbl2 = v2.get("DisciplineName") or v2.get("EventName") or v2.get("Name")
                            ev_key = wa_guess_event_key(lbl2 or "")
                            if ev_key:
                                break

                t_str = str(time_val) if time_val is not None else ""
                d_iso = None
                if isinstance(date_val, str):
                    d_iso = parse_iso_date(date_val)
                elif isinstance(date_val, (datetime, date)):
                    d_iso = date_val if isinstance(date_val, date) else date_val.date()

                # Meet/location best-effort
                meet = x.get("CompetitionName") or x.get("Meet") or x.get("Competition")
                if isinstance(meet, dict):
                    meet = meet.get("Name")
                location = x.get("City") or x.get("Location") or x.get("Venue")
                if isinstance(location, dict):
                    location = location.get("Name") or location.get("City")

                if ev_key and d_iso and wa_is_lcm_50m(x):
                    rows.append({
                        "event": ev_key,
                        "time": t_str,
                        "seconds": wa_time_to_seconds(t_str),
                        "date": d_iso,
                        "meet": str(meet) if meet is not None else None,
                        "location": str(location) if location is not None else None,
                    })

            # Recurse
            for v in x.values():
                visit(v)

        elif isinstance(x, list):
            for it in x:
                visit(it)

    visit(js)
    return rows


def wa_compute_pool_bests(rows: List[Dict[str, Any]], ow_date: date) -> Dict[str, Dict[str, Optional[str]]]:
    """Compute SB_YTD and PB (up to OW date) from extracted rows."""
    out: Dict[str, Dict[str, Optional[str]]] = {e: {} for e in POOL_EVENTS}

    for ev in POOL_EVENTS:
        ev_rows = [r for r in rows if r.get("event") == ev and r.get("seconds") is not None and r.get("date") and r["date"] <= ow_date]
        if not ev_rows:
            continue

        best_pb = min(ev_rows, key=lambda x: x["seconds"])
        out[ev]["pb_upto_time"] = best_pb.get("time")
        out[ev]["pb_upto_date"] = best_pb.get("date").isoformat() if best_pb.get("date") else None
        out[ev]["pb_upto_meet"] = best_pb.get("meet")
        out[ev]["pb_upto_location"] = best_pb.get("location")

        ev_ytd = [r for r in ev_rows if r.get("date") and r["date"].year == ow_date.year]
        if ev_ytd:
            best_ytd = min(ev_ytd, key=lambda x: x["seconds"])
            out[ev]["sb_ytd_time"] = best_ytd.get("time")
            out[ev]["sb_ytd_date"] = best_ytd.get("date").isoformat() if best_ytd.get("date") else None
            out[ev]["sb_ytd_meet"] = best_ytd.get("meet")
            out[ev]["sb_ytd_location"] = best_ytd.get("location")

    return out


# =======================
# 1) World Aquatics: OW competition + events + results
# =======================
def fetch_competition_meta(competition_id: str) -> Dict[str, Any]:
    url = f"{FINA_BASE}/competitions/{competition_id}/events"
    data = http_get_json(url, HEADERS_WA)
    return data if isinstance(data, dict) else {}


def fetch_ow_events(competition_meta: Dict[str, Any]) -> List[Dict[str, Any]]:
    ow_events: List[Dict[str, Any]] = []
    for sport in competition_meta.get("Sports", []):
        if sport.get("Code") != "OW":
            continue
        for d in sport.get("DisciplineList", []):
            ow_events.append({
                "name": d.get("DisciplineName", ""),
                "gender": d.get("Gender", ""),
                "id": d.get("Id", ""),
            })
    return ow_events


def pick_10km_events(ow_events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not ow_events:
        return []

    if not AUTO_PICK_10KM:
        # interactive selection if you ever want it
        print("\nOW events:")
        for i, e in enumerate(ow_events, 1):
            print(f"{i}. {e['name']} ({e['gender']}) -> {e['id']}")
        idx = int(input("Pick event: ")) - 1
        return [ow_events[idx]]

    picked = []
    for e in ow_events:
        nm = (e.get("name") or "").lower()
        if "10" in nm and "km" in nm:
            picked.append(e)
    return picked


def fetch_event_results(event_id: str) -> List[Dict[str, Any]]:
    """
    Returns athletes for OW event:
    wa_id, full_name, nat, sex (from event gender), ow_time, ow_rank
    """
    url = f"{FINA_BASE}/events/{event_id}"
    data = http_get_json(url, HEADERS_WA)

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
            "ow_time": a.get("Time", ""),
            "ow_rank": a.get("Rank", ""),
        })
    return out


# =======================
# 2) World Aquatics: Athlete profile + (optional) pool results via WA
# =======================
def fetch_wa_profile(wa_id: str) -> Dict[str, Any]:
    """Try several candidate endpoints; return {} if none works."""
    if not wa_id:
        return {}

    candidates = [
        f"{FINA_BASE}/persons/{wa_id}",
        f"{FINA_BASE}/person/{wa_id}",
        f"{FINA_BASE}/athletes/{wa_id}",
        f"{FINA_BASE}/athlete/{wa_id}",
        f"{FINA_BASE}/competitors/{wa_id}",
        f"{FINA_BASE}/competitor/{wa_id}",
        f"{FINA_BASE}/profiles/{wa_id}",
        f"{FINA_BASE}/profile/{wa_id}",
    ]

    for url in candidates:
        try:
            js = http_get_json(url, HEADERS_WA, retries=1, pause=0.2)
            if isinstance(js, dict) and js:
                return js
        except Exception:
            continue
    return {}


def fetch_wa_pool_best_attempt(wa_id: str, ow_date: date) -> Dict[str, Dict[str, Optional[str]]]:
    """
    OPTIONAL: Try to pull pool results from WA using candidate endpoints.
    Returns dict like:
    {
      "400 Free": {"sb_ytd_time":..., "sb_ytd_date":..., "sb_ytd_meet":..., "pb_time":..., ...},
      ...
    }
    If endpoints don't exist, returns {} and we fallback to SwimRankings.
    """
    if not wa_id:
        return {}

    # Candidate endpoints that *may* exist depending on sport setup.
    candidates = [
        f"{FINA_BASE}/persons/{wa_id}/results",
        f"{FINA_BASE}/person/{wa_id}/results",
        f"{FINA_BASE}/athletes/{wa_id}/results",
        f"{FINA_BASE}/athlete/{wa_id}/results",
    ]

    # Store raw JSON when an endpoint responds, to allow manual inspection.
    debug_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "debug_wa_pool")
    os.makedirs(debug_dir, exist_ok=True)

    for url in candidates:
        try:
            js = http_get_json(url, HEADERS_WA, retries=1, pause=0.2)

            # Accept dict or list payloads
            if not js:
                continue

            lg.info("WA pool endpoint responded: %s", url)

            # Save raw payload for later mapping verification
            safe_id = str(wa_id).replace("/", "_")
            raw_path = os.path.join(debug_dir, f"wa_pool_{safe_id}.json")
            try:
                import json as _json
                with open(raw_path, "w", encoding="utf-8") as f:
                    _json.dump(js, f, ensure_ascii=False, indent=2, default=str)
            except Exception as e:
                lg.warning("Failed to write WA raw pool json for %s: %s", wa_id, e)

            # Try to extract pool-like rows and compute bests
            rows = wa_extract_pool_rows(js)
            if not rows:
                lg.warning("WA pool mapping: no recognizable pool rows for wa_id=%s from %s", wa_id, url)
                return {}

            mapped = wa_compute_pool_bests(rows, ow_date)
            return mapped

        except Exception:
            continue

    return {}

# =======================
# 3) SwimRankings: find athleteId + PB all-time + SB_YTD (scrape)
# =======================
_SR_ID_CACHE: Dict[Tuple[str, str], Optional[int]] = {}
_PB_ALL_CACHE: Dict[int, Dict[str, Dict[str, Optional[str]]]] = {}
_SB_YTD_CACHE: Dict[Tuple[int, int, str], Dict[str, Dict[str, Optional[str]]]] = {}


def swimrankings_find_athlete_id(full_name: str, nat: str) -> Optional[int]:
    """
    Find SwimRankings athlete id using the swimrankings library search.
    Returns athleteId int or None.
    """
    if not SWIMRANKINGS_LIB:
        return None

    key = (full_name, nat)
    if key in _SR_ID_CACHE:
        return _SR_ID_CACHE[key]

    last_raw, first_raw = split_last_first(full_name)
    last = norm_name(last_raw).upper()
    first = norm_name(first_raw)

    if not last:
        _SR_ID_CACHE[key] = None
        return None

    try:
        athletes = Athletes(name=last)
    except Exception:
        _SR_ID_CACHE[key] = None
        return None

    matches = []
    for ath in athletes:
        try:
            ath_country = getattr(ath, "country", "")
            ath_first = getattr(ath, "first_name", "")
            ath_id = getattr(ath, "athlete_id", None) or getattr(ath, "id", None)
        except Exception:
            continue

        if nat and ath_country != nat:
            continue
        if first and ath_first.lower() != first.lower():
            continue
        if ath_id is None:
            continue
        matches.append(ath)

    if not matches:
        # fallback: try without nat
        for ath in athletes:
            try:
                ath_first = getattr(ath, "first_name", "")
                ath_id = getattr(ath, "athlete_id", None) or getattr(ath, "id", None)
            except Exception:
                continue
            if first and ath_first.lower() != first.lower():
                continue
            if ath_id is None:
                continue
            matches.append(ath)

    if not matches:
        _SR_ID_CACHE[key] = None
        return None

    ath = matches[0]
    ath_id = getattr(ath, "athlete_id", None) or getattr(ath, "id", None)
    _SR_ID_CACHE[key] = int(ath_id) if ath_id is not None else None
    return _SR_ID_CACHE[key]


def swimrankings_pb_alltime(athlete_id: int) -> Dict[str, Dict[str, Optional[str]]]:
    """
    PB All-time LCM for 400/800/1500 free.
    Uses swimrankings library details.personal_bests.
    """
    if athlete_id in _PB_ALL_CACHE:
        return _PB_ALL_CACHE[athlete_id]

    out: Dict[str, Dict[str, Optional[str]]] = {}
    if not SWIMRANKINGS_LIB:
        _PB_ALL_CACHE[athlete_id] = out
        return out

    # Rebuild athlete object by searching by id is not provided by lib,
    # so we scrape PB all-time via page as fallback if needed.
    # BUT: easiest is to reuse scraping for PB too.
    # For now we do scraping-only for reliability:
    _PB_ALL_CACHE[athlete_id] = out
    return out


def sr_time_to_seconds(t: str) -> Optional[float]:
    """
    Convert times like '3:55.12' or '15:01.34' to seconds.
    """
    if not t:
        return None
    t = t.strip()
    try:
        if ":" in t:
            mm, rest = t.split(":", 1)
            return float(mm) * 60 + float(rest)
        return float(t)
    except Exception:
        return None


def parse_sr_date(s: str) -> Optional[date]:
    """
    SwimRankings often uses dd.mm.yyyy
    """
    if not s:
        return None
    s = s.strip()
    for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    return None


def swimrankings_scrape_pool_bests(athlete_id: int, ow_date: date) -> Dict[str, Dict[str, Optional[str]]]:
    """
    Scrape athlete page and compute:
    - SB_YTD: best result in OW year up to ow_date (LCM) for 400/800/1500 Free
    - PB_AllTime: best result overall up to ow_date (LCM) for those events

    IMPORTANT: The exact table structure on swimrankings.net can vary.
    This parser is defensive and may need small tweaks based on what you see.
    """
    key = (athlete_id, ow_date.year, ow_date.isoformat())
    if key in _SB_YTD_CACHE:
        return _SB_YTD_CACHE[key]

    url = SWIMRANKINGS_ATHLETE_URL.format(athlete_id=athlete_id)
    html = http_get_text(url, HEADERS_SR, retries=2, pause=0.5)
    soup = BeautifulSoup(html, "lxml")

    # Heuristic: search all rows that look like results with event + time + date
    # We'll collect candidate rows then compute minima.
    candidates = []

    # Any table rows
    for tr in soup.find_all("tr"):
        tds = [td.get_text(" ", strip=True) for td in tr.find_all(["td", "th"])]
        if len(tds) < 4:
            continue

        row_text = " | ".join(tds).lower()

        # must include one of distances and "free"/"freestyle" hints (varies by language)
        is_target = False
        for dist in ("400", "800", "1500"):
            if dist in row_text and ("free" in row_text or "freestyle" in row_text or "fr" in row_text):
                is_target = True
                break
        if not is_target:
            continue

        # try to find a time token like 3:55.12 or 15:01.34
        time_token = None
        for cell in tds:
            if re.match(r"^\d{1,2}:\d{2}\.\d{2}$", cell) or re.match(r"^\d{1,2}:\d{2}\.\d{1,2}$", cell):
                time_token = cell
                break
        if not time_token:
            continue

        # try date token dd.mm.yyyy
        date_token = None
        for cell in tds:
            if re.match(r"^\d{2}\.\d{2}\.\d{4}$", cell):
                date_token = cell
                break
        if not date_token:
            continue

        d = parse_sr_date(date_token)
        if not d:
            continue

        # course hint: LCM/SCM maybe present; we need LCM (50m)
        # if there is explicit 'LCM' accept; if explicit 'SCM' reject; else keep (site sometimes is LCM default)
        if "scm" in row_text or "25m" in row_text:
            continue

        # event label
        # take best guess from first column
        event_label = tds[0]
        event_label_low = event_label.lower()
        event_key = None
        if "400" in event_label_low:
            event_key = "400 Free"
        elif "800" in event_label_low:
            event_key = "800 Free"
        elif "1500" in event_label_low:
            event_key = "1500 Free"
        else:
            continue

        candidates.append({
            "event": event_key,
            "time": time_token,
            "seconds": sr_time_to_seconds(time_token),
            "date": d,
            "meet": tds[2] if len(tds) > 2 else None,
            "location": tds[3] if len(tds) > 3 else None,
        })

    # Compute SB_YTD and PB up to OW date
    out: Dict[str, Dict[str, Optional[str]]] = {e: {} for e in POOL_EVENTS}

    for ev in POOL_EVENTS:
        ev_rows = [r for r in candidates if r["event"] == ev and r["seconds"] is not None and r["date"] <= ow_date]
        if not ev_rows:
            continue

        # PB up to ow_date
        best_pb = min(ev_rows, key=lambda x: x["seconds"])
        out[ev]["pb_upto_time"] = best_pb["time"]
        out[ev]["pb_upto_date"] = best_pb["date"].isoformat()
        out[ev]["pb_upto_meet"] = best_pb.get("meet")
        out[ev]["pb_upto_location"] = best_pb.get("location")

        # SB_YTD: same year as OW date
        ev_ytd = [r for r in ev_rows if r["date"].year == ow_date.year]
        if ev_ytd:
            best_ytd = min(ev_ytd, key=lambda x: x["seconds"])
            out[ev]["sb_ytd_time"] = best_ytd["time"]
            out[ev]["sb_ytd_date"] = best_ytd["date"].isoformat()
            out[ev]["sb_ytd_meet"] = best_ytd.get("meet")
            out[ev]["sb_ytd_location"] = best_ytd.get("location")

    _SB_YTD_CACHE[key] = out
    return out


# =======================
# Main join
# =======================
def main() -> None:
    meta = fetch_competition_meta(COMPETITION_ID)
    comp_name = meta.get("Name", "")
    comp_from = parse_iso_date(meta.get("From", ""))
    comp_to = parse_iso_date(meta.get("To", ""))

    lg.info("Competition: %s (%s -> %s)", comp_name, comp_from, comp_to)

    ow_events = fetch_ow_events(meta)
    picked = pick_10km_events(ow_events)
    if not picked:
        lg.error("No 10km OW events found.")
        return

    rows: List[Dict[str, Any]] = []

    # We need the OW race date. If you know it precisely per event, you can set it here.
    # For now we use competition 'To' as approximation, fallback 'From'.
    ow_date = comp_to or comp_from
    if not ow_date:
        lg.error("Cannot determine OW date (competition From/To missing).")
        return

    for ev in picked:
        ev_name = ev["name"]
        ev_gender = ev.get("gender", "")
        ev_id = ev["id"]

        lg.info("OW event: %s (%s) id=%s", ev_name, ev_gender, ev_id)
        athletes = fetch_event_results(ev_id)
        lg.info("Athletes: %d", len(athletes))

        for a in athletes:
            wa_id = a.get("wa_id", "")
            full_name = a.get("full_name", "")
            nat = a.get("nat", "")
            ow_time = a.get("ow_time", "")
            ow_rank = a.get("ow_rank", "")

            # --- WA profile (optional info)
            profile = fetch_wa_profile(wa_id)
            # try common keys for sex or birth year if present
            wa_sex = profile.get("Gender") or profile.get("Sex") or ev_gender
            wa_birth = profile.get("BirthDate") or profile.get("DateOfBirth") or profile.get("BirthYear")

            # --- WA pool (optional) - currently stub unless endpoint exists
            wa_pool = fetch_wa_pool_best_attempt(wa_id, ow_date)

            # --- SwimRankings pool
            sr_id = swimrankings_find_athlete_id(full_name, nat)
            sr_pool = {e: {} for e in POOL_EVENTS}
            if sr_id:
                try:
                    sr_pool = swimrankings_scrape_pool_bests(sr_id, ow_date)
                except Exception as e:
                    lg.warning("SwimRankings scrape failed for %s (%s): %s", full_name, sr_id, e)

            time.sleep(SLEEP_BETWEEN_ATHLETES)

            for pool_event in POOL_EVENTS:
                sr_ev = sr_pool.get(pool_event, {})
                wa_ev = wa_pool.get(pool_event, {}) if isinstance(wa_pool, dict) else {}

                rows.append({
                    "Competition": comp_name,
                    "OW_Event": ev_name,
                    "OW_EventGender": ev_gender,
                    "OW_Date": ow_date.isoformat(),

                    "WA_ID": wa_id,
                    "Athlete": full_name,
                    "NAT": nat,
                    "Sex": wa_sex,
                    "Birth": wa_birth,

                    "OW_Rank": ow_rank,
                    "OW_Time": ow_time,

                    "PoolEvent": pool_event,

                    # ---- World Aquatics (if available)
                    "WA_SB_YTD_Time": wa_ev.get("sb_ytd_time"),
                    "WA_SB_YTD_Date": wa_ev.get("sb_ytd_date"),
                    "WA_SB_YTD_Meet": wa_ev.get("sb_ytd_meet"),
                    "WA_SB_YTD_Location": wa_ev.get("sb_ytd_location"),

                    "WA_PB_Upto_Time": wa_ev.get("pb_upto_time"),
                    "WA_PB_Upto_Date": wa_ev.get("pb_upto_date"),
                    "WA_PB_Upto_Meet": wa_ev.get("pb_upto_meet"),
                    "WA_PB_Upto_Location": wa_ev.get("pb_upto_location"),

                    # ---- SwimRankings (scrape)
                    "SR_ID": sr_id,
                    "SR_SB_YTD_Time": sr_ev.get("sb_ytd_time"),
                    "SR_SB_YTD_Date": sr_ev.get("sb_ytd_date"),
                    "SR_SB_YTD_Meet": sr_ev.get("sb_ytd_meet"),
                    "SR_SB_YTD_Location": sr_ev.get("sb_ytd_location"),

                    "SR_PB_Upto_Time": sr_ev.get("pb_upto_time"),
                    "SR_PB_Upto_Date": sr_ev.get("pb_upto_date"),
                    "SR_PB_Upto_Meet": sr_ev.get("pb_upto_meet"),
                    "SR_PB_Upto_Location": sr_ev.get("pb_upto_location"),
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