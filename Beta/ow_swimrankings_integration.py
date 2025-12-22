# API_WorldAquatics_OW_Pool_Results_Integration_Beta
# Integrate World Aquatics Open Water results with Pool results from World Aquatics endpoints
# TODO: sync con API_Competitions
# Gestione Temporale PB Y SB
# schema di incremento % diviso per ogni competizione

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

# =======================
# SETTINGS
# =======================
COMPETITION_ID = "4725"
AUTO_PICK_10KM = True
OUTPUT_XLSX = "ow_pool_join.xlsx"

SLEEP_BETWEEN_ATHLETES = 0.2
SLEEP_BETWEEN_REQUESTS = 0.2  # (al momento non usata direttamente)

# target pool events
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

FINA_BASE = "https://api.worldaquatics.com/fina"

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
    """(Non usata dopo la rimozione SwimRankings, ma la lascio per compatibilità)"""
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
    try:
        return datetime.fromisoformat(s.replace("Z", "")).date()
    except Exception:
        pass
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
    """
    STRICT check: accept ONLY 50m pool / LCM.
    - True  -> explicit 50m / LCM
    - False -> 25m / SCM or unknown
    """
    found_50 = False

    for k in ("PoolLength", "Pool", "PoolSize", "PoolConfiguration", "Course", "CourseCode"):
        v = d.get(k)
        if v is None:
            continue

        s = str(v).lower().strip()

        # --- explicit short course → reject
        if "25" in s or "scm" in s or "short" in s:
            return False

        # --- explicit long course → accept
        if s in {"lcm", "50", "50m", "50 m", "50-m"}:
            found_50 = True
        elif "50" in s and "25" not in s:
            found_50 = True
        elif "lcm" in s or "long" in s:
            found_50 = True

    # accept ONLY if we explicitly found a 50m / LCM signal
    return found_50

def wa_extract_pool_rows(js: Any) -> List[Dict[str, Any]]:
    """Recursively scan WA JSON and extract pool result-like rows."""
    rows: List[Dict[str, Any]] = []

    def _pick_first(d: Dict[str, Any], keys: Tuple[str, ...]) -> Optional[str]:
        for kk in keys:
            vv = d.get(kk)
            if vv is None:
                continue
            sv = str(vv).strip()
            if sv:
                return sv
        return None

    def visit(x: Any) -> None:
        if isinstance(x, dict):
            time_val = x.get("Time") or x.get("Result") or x.get("SwimTime") or x.get("Performance")
            date_val = x.get("Date") or x.get("StartDate") or x.get("CompetitionDate") or x.get("From")
            label_val = (
                x.get("DisciplineName")
                or x.get("EventName")
                or x.get("Name")
                or x.get("Event")
                or x.get("Discipline")
            )

            if time_val and (date_val or label_val):
                label = str(label_val) if label_val is not None else ""
                ev_key = wa_guess_event_key(label)

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

                # -----------------------------
                # MEET (robust)
                # -----------------------------
                meet_obj = x.get("CompetitionName") or x.get("Meet") or x.get("Competition") or x.get("Event")
                meet_name = None
                if isinstance(meet_obj, dict):
                    meet_name = (
                        meet_obj.get("Name")
                        or meet_obj.get("CompetitionName")
                        or meet_obj.get("OfficialName")
                        or meet_obj.get("EventName")
                    )
                    if meet_name is not None:
                        meet_name = str(meet_name).strip() or None
                else:
                    meet_name = str(meet_obj).strip() if meet_obj is not None else None

                # -----------------------------
                # LOCATION / COUNTRY (robust)
                # -----------------------------
                # 1) direct keys on the row
                loc_city = _pick_first(x, ("City", "Town", "Place"))
                loc_country = _pick_first(x, ("CountryName", "CompetitionCountry", "Country", "NationName"))
                loc_cc = _pick_first(x, ("CountryCode", "CompetitionCountryCode", "NationCode"))

                # 2) nested competition/meet/event blocks
                for parent_key in ("Competition", "Meet", "Event", "Race"):
                    parent = x.get(parent_key)
                    if isinstance(parent, dict):
                        loc_city = loc_city or _pick_first(parent, ("City", "Town", "Place"))
                        loc_country = loc_country or _pick_first(parent, ("CountryName", "CompetitionCountry", "Country", "NationName"))
                        loc_cc = loc_cc or _pick_first(parent, ("CountryCode", "CompetitionCountryCode", "NationCode"))

                # 3) Venue/Location dict fallback
                loc_obj = x.get("Location") or x.get("Venue")
                if isinstance(loc_obj, dict):
                    loc_city = loc_city or _pick_first(loc_obj, ("City", "Name"))
                    loc_country = loc_country or _pick_first(loc_obj, ("CountryName", "Country"))
                    loc_cc = loc_cc or _pick_first(loc_obj, ("CountryCode",))

                # Compose strings
                competition_country = None
                if loc_country and loc_cc:
                    competition_country = f"{loc_country} ({loc_cc})"
                elif loc_country:
                    competition_country = loc_country
                elif loc_cc:
                    competition_country = loc_cc

                location_str = None
                if loc_city and competition_country:
                    location_str = f"{loc_city}, {competition_country}"
                elif loc_city:
                    location_str = loc_city
                elif competition_country:
                    location_str = competition_country

                # -----------------------------
                # Add row
                # -----------------------------
                if ev_key and d_iso and wa_is_lcm_50m(x):
                    rows.append({
                        "event": ev_key,
                        "time": t_str,
                        "seconds": wa_time_to_seconds(t_str),
                        "date": d_iso,
                        "meet": meet_name,
                        "location": location_str,
                        "competition_country": competition_country,
                    })

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
        ev_rows = [
            r for r in rows
            if r.get("event") == ev
            and r.get("seconds") is not None
            and r.get("date")
            and r["date"] <= ow_date
        ]
        if not ev_rows:
            continue

        best_pb = min(ev_rows, key=lambda x: x["seconds"])
        out[ev]["pb_upto_time"] = best_pb.get("time")
        out[ev]["pb_upto_date"] = best_pb.get("date").isoformat() if best_pb.get("date") else None
        out[ev]["pb_upto_meet"] = best_pb.get("meet")
        out[ev]["pb_upto_location"] = best_pb.get("location")
        out[ev]["pb_upto_country"] = best_pb.get("competition_country")

        ev_ytd = [r for r in ev_rows if r.get("date") and r["date"].year == ow_date.year]
        if ev_ytd:
            best_ytd = min(ev_ytd, key=lambda x: x["seconds"])
            out[ev]["sb_ytd_time"] = best_ytd.get("time")
            out[ev]["sb_ytd_date"] = best_ytd.get("date").isoformat() if best_ytd.get("date") else None
            out[ev]["sb_ytd_meet"] = best_ytd.get("meet")
            out[ev]["sb_ytd_location"] = best_ytd.get("location")
            out[ev]["sb_ytd_country"] = best_ytd.get("competition_country")


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
    """Returns athletes for OW event: wa_id, full_name, nat, ow_time, ow_rank"""
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
# 2) World Aquatics: Athlete profile + pool results via WA
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
    Pull pool results from WA using candidate endpoints.
    Returns per event: SB_YTD and PB up to OW date.
    """
    if not wa_id:
        return {}

    candidates = [
        f"{FINA_BASE}/persons/{wa_id}/results",
        f"{FINA_BASE}/person/{wa_id}/results",
        f"{FINA_BASE}/athletes/{wa_id}/results",
        f"{FINA_BASE}/athlete/{wa_id}/results",
    ]

    for url in candidates:
        try:
            js = http_get_json(url, HEADERS_WA, retries=1, pause=0.2)
            if not js:
                continue

            lg.info("WA pool endpoint responded: %s", url)

            rows = wa_extract_pool_rows(js)
            if not rows:
                lg.warning("WA pool mapping: no recognizable pool rows for wa_id=%s from %s", wa_id, url)
                return {}

            return wa_compute_pool_bests(rows, ow_date)

        except Exception:
            continue

    return {}


# =======================
# Main join
# =======================
def main() -> None:
    meta = fetch_competition_meta(COMPETITION_ID)
    comp_name = meta.get("Name", "")
    comp_from = parse_iso_date(meta.get("From", ""))
    comp_to = parse_iso_date(meta.get("To", ""))
    ow_country_code = meta.get("CountryCode")

    lg.info("Competition: %s (%s -> %s)", comp_name, comp_from, comp_to)

    ow_events = fetch_ow_events(meta)
    picked = pick_10km_events(ow_events)
    if not picked:
        lg.error("No 10km OW events found.")
        return

    rows: List[Dict[str, Any]] = []

    # OW date approximation: comp_to fallback comp_from
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

            # skip athletes without a rank (DNF/DNS/DSQ)
            if not ow_rank:
                continue

            profile = fetch_wa_profile(wa_id)
            wa_sex = profile.get("Gender") or profile.get("Sex") or ev_gender
            wa_birth = (
                profile.get("DOB")
                or profile.get("DateOfBirth")
                or profile.get("BirthDate")
                or profile.get("YearOfBirth")
                or profile.get("BirthYear")
            )

            wa_pool = fetch_wa_pool_best_attempt(wa_id, ow_date)

            time.sleep(SLEEP_BETWEEN_ATHLETES)

            for pool_event in POOL_EVENTS:
                wa_ev = wa_pool.get(pool_event, {}) if isinstance(wa_pool, dict) else {}

                rows.append({
                    "Competition": comp_name,
                    "Country": ow_country_code,
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

                    "WA_SB_YTD_Time": wa_ev.get("sb_ytd_time"),
                    "WA_SB_YTD_Date": wa_ev.get("sb_ytd_date"),
                    "WA_SB_YTD_Meet": wa_ev.get("sb_ytd_meet"),
                    # "WA_SB_YTD_Location": wa_ev.get("sb_ytd_location"),
                    "WA_SB_YTD_Country": wa_ev.get("sb_ytd_country"),

                    "WA_PB_Upto_Time": wa_ev.get("pb_upto_time"),
                    "WA_PB_Upto_Date": wa_ev.get("pb_upto_date"),
                    "WA_PB_Upto_Meet": wa_ev.get("pb_upto_meet"),
                    # "WA_PB_Upto_Location": wa_ev.get("pb_upto_location"),
                    "WA_PB_Upto_Country": wa_ev.get("pb_upto_country"),
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
