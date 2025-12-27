# API_WorldAquatics_OW_Pool_Results_Integration_Beta (MULTI COMPETITIONS + PROGRESS + SINGLE CSV)
from __future__ import annotations

import os
import sys
import time
import json
import glob
import shutil
import logging
import threading
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pandas as pd

try:
    import orjson  # fast json
    HAS_ORJSON = True
except Exception:
    HAS_ORJSON = False

# =======================
# SETTINGS
# =======================
AUTO_PICK_10KM = True

# Output
WRITE_XLSX = False

# Cache
CACHE_TTL_SECONDS: Optional[int] = 30 * 24 * 3600  # None -> never refresh
CLEANUP_CACHE_AT_END = False

# Debug
LIMIT_ATHLETES = 10  # None or 0 -> no limit

# Concurrency
MAX_WORKERS = 8
REQUEST_TIMEOUT = 30
HTTP_RETRIES = 3
HTTP_BACKOFF = 0.6

# Optional pacing
SLEEP_BETWEEN_ATHLETES = 0.0
SLEEP_BETWEEN_REQUESTS = 0.0

# Pool events
POOL_EVENTS = ["400 Free", "800 Free", "1500 Free"]

# =======================
# PATHS
# =======================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_POOL_DIR = os.path.join(BASE_DIR, "cache_wa_pool_rows")
CACHE_PROFILE_DIR = os.path.join(BASE_DIR, "cache_wa_profile")
os.makedirs(CACHE_POOL_DIR, exist_ok=True)
os.makedirs(CACHE_PROFILE_DIR, exist_ok=True)

# =======================
# LOGGING
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

# Thread-local session
_THREAD_LOCAL = threading.local()

# In-memory caches
_WA_POOL_ROWS_CACHE: Dict[str, List[Dict[str, Any]]] = {}
_WA_PROFILE_CACHE: Dict[str, Dict[str, Any]] = {}
_POOL_LOCK = threading.Lock()
_PROFILE_LOCK = threading.Lock()

# Progress print lock
_PRINT_LOCK = threading.Lock()


# =======================
# Progress helpers
# =======================
def _bar(done: int, total: int, width: int = 24) -> str:
    if total <= 0:
        return "[" + "." * width + "]  0%"
    ratio = min(max(done / total, 0.0), 1.0)
    filled = int(ratio * width)
    return "[" + "#" * filled + "." * (width - filled) + f"] {int(ratio * 100):3d}%"


def _print_progress(section_label: str, sec_done: int, sec_total: int,
                    global_done: int, global_total: int) -> None:
    line1 = f"{section_label:<18} {_bar(sec_done, sec_total)}  ({sec_done}/{sec_total})"
    line2 = f"{'Competition:':<18} {_bar(global_done, global_total)}  ({global_done}/{global_total})"
    with _PRINT_LOCK:
        sys.stdout.write("\x1b[2A")  # up 2 lines
        sys.stdout.write("\r" + line1 + " " * 10 + "\n")
        sys.stdout.write("\r" + line2 + " " * 10 + "\n")
        sys.stdout.flush()


# =======================
# HTTP + cache helpers
# =======================
def _get_session() -> requests.Session:
    sess = getattr(_THREAD_LOCAL, "session", None)
    if sess is None:
        sess = requests.Session()
        _THREAD_LOCAL.session = sess
    return sess


def cleanup_cache_dirs() -> None:
    if os.path.isdir(CACHE_PROFILE_DIR):
        shutil.rmtree(CACHE_PROFILE_DIR, ignore_errors=True)
    if os.path.isdir(CACHE_POOL_DIR):
        shutil.rmtree(CACHE_POOL_DIR, ignore_errors=True)


def _is_cache_fresh(path: str, ttl_seconds: Optional[int]) -> bool:
    if ttl_seconds is None:
        return os.path.exists(path)
    if not os.path.exists(path):
        return False
    try:
        age = time.time() - os.path.getmtime(path)
        return age <= ttl_seconds
    except Exception:
        return False


def http_get_json(url: str, headers: dict) -> Any:
    last_err: Optional[Exception] = None
    sess = _get_session()

    for i in range(HTTP_RETRIES):
        try:
            r = sess.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            if SLEEP_BETWEEN_REQUESTS:
                time.sleep(SLEEP_BETWEEN_REQUESTS)
            return r.json()
        except Exception as e:
            last_err = e
            time.sleep(HTTP_BACKOFF * (2 ** i))

    raise RuntimeError(f"GET failed: {url} -> {last_err}")


def _read_json_file(path: str) -> Any:
    try:
        if HAS_ORJSON:
            with open(path, "rb") as f:
                return orjson.loads(f.read())
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _write_json_file(path: str, obj: Any) -> None:
    try:
        if HAS_ORJSON:
            def _default(o: Any):
                if isinstance(o, (date, datetime)):
                    return o.isoformat()
                return str(o)

            data = orjson.dumps(obj, option=orjson.OPT_INDENT_2, default=_default)
            with open(path, "wb") as f:
                f.write(data)
            return

        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2, default=str)
    except Exception:
        pass


# =======================
# Generic helpers
# =======================
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


def _to_date(v: Any) -> Optional[date]:
    if v is None:
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, str):
        return parse_iso_date(v)
    return None


# =======================
# Pool mapping
# =======================
def wa_time_to_seconds(t: str) -> Optional[float]:
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


def wa_course_from_text(*texts: Optional[str]) -> Optional[str]:
    joined = " | ".join([str(t) for t in texts if t]).lower()
    if "25m" in joined or "25 m" in joined or "scm" in joined or "(25" in joined:
        return "SCM"
    if "50m" in joined or "50 m" in joined or "lcm" in joined or "(50" in joined:
        return "LCM"
    return None


def wa_extract_pool_rows(js: Any) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    def _pick_comp_name(d: Dict[str, Any]) -> Optional[str]:
        v = d.get("CompetitionName") or d.get("OfficialName") or d.get("Name") or d.get("Title")
        if v is None:
            return None
        s = str(v).strip()
        return s or None

    def _pick_country(d: Dict[str, Any]) -> Optional[str]:
        """Best-effort: return 3-letter meet country code (e.g., ITA) from any known WA fields."""

        def _norm3(v: Any) -> Optional[str]:
            if v is None:
                return None
            s = str(v).strip().upper()
            return s if len(s) == 3 and s.isalpha() else None

        # Common direct keys (various WA payload variants)
        for k in (
            "CountryCode", "countryCode",
            "HostCountryCode", "hostCountryCode",
            "VenueCountryCode", "venueCountryCode",
            "MeetCountryCode", "meetCountryCode",
        ):
            cc = _norm3(d.get(k))
            if cc:
                return cc

        # Sometimes nested under Location/Venue/Competition/etc.
        for k in ("Location", "location", "Venue", "venue", "Competition", "competition", "Meet", "meet"):
            obj = d.get(k)
            if isinstance(obj, dict):
                cc = _pick_country(obj)
                if cc:
                    return cc

        # Very generic fallback: scan for any key containing 'country' with a 3-letter code value
        for k, v in d.items():
            if "country" in str(k).lower():
                cc = _norm3(v)
                if cc:
                    return cc

        return None

    def visit(x: Any, parents: List[Dict[str, Any]]) -> None:
        if isinstance(x, dict):
            time_val = x.get("Time") or x.get("Result") or x.get("SwimTime") or x.get("Performance")
            date_val = x.get("Date") or x.get("StartDate") or x.get("CompetitionDate") or x.get("From")
            label_val = x.get("DisciplineName") or x.get("EventName") or x.get("Name") or x.get("Event") or x.get("Discipline")

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

                d_iso = _to_date(date_val)

                t_str = str(time_val) if time_val is not None else ""

                comp_names: List[Optional[str]] = [_pick_comp_name(x)]
                comp_countries: List[Optional[str]] = [_pick_country(x)]

                for k in ("Competition", "Meet", "Event", "Race"):
                    obj = x.get(k)
                    if isinstance(obj, dict):
                        comp_names.append(_pick_comp_name(obj))
                        comp_countries.append(_pick_country(obj))

                for p in reversed(parents):
                    if isinstance(p, dict):
                        comp_names.append(_pick_comp_name(p))
                        comp_countries.append(_pick_country(p))

                course = wa_course_from_text(*comp_names)
                if ev_key and d_iso and course == "LCM":
                    rows.append({
                        "event": ev_key,
                        "time": t_str,
                        "seconds": wa_time_to_seconds(t_str),
                        "date": d_iso,
                        "meet": next((c for c in comp_names if c), None),
                        "country": next((c for c in comp_countries if c), None),
                    })

            new_parents = parents + [x]
            for v in x.values():
                visit(v, new_parents)

        elif isinstance(x, list):
            for it in x:
                visit(it, parents)

    visit(js, [])
    return rows


def wa_compute_pool_bests(rows: List[Dict[str, Any]], ow_date: date) -> Dict[str, Dict[str, Optional[str]]]:
    out: Dict[str, Dict[str, Optional[str]]] = {e: {} for e in POOL_EVENTS}

    for ev in POOL_EVENTS:
        ev_rows: List[Dict[str, Any]] = []
        for r in rows:
            if r.get("event") != ev:
                continue
            if r.get("seconds") is None:
                continue

            d = _to_date(r.get("date"))
            if not d or d > ow_date:
                continue

            rr = dict(r)
            rr["date"] = d
            ev_rows.append(rr)

        if not ev_rows:
            continue

        best_pb = min(ev_rows, key=lambda x: x["seconds"])
        out[ev]["pb_upto_time"] = best_pb.get("time")
        out[ev]["pb_upto_date"] = best_pb["date"].isoformat() if best_pb.get("date") else None
        out[ev]["pb_upto_meet"] = best_pb.get("meet")
        out[ev]["pb_upto_country"] = best_pb.get("country")

        ev_ytd = [r for r in ev_rows if r.get("date") and r["date"].year == ow_date.year]
        if ev_ytd:
            best_ytd = min(ev_ytd, key=lambda x: x["seconds"])
            out[ev]["sb_ytd_time"] = best_ytd.get("time")
            out[ev]["sb_ytd_date"] = best_ytd["date"].isoformat() if best_ytd.get("date") else None
            out[ev]["sb_ytd_meet"] = best_ytd.get("meet")
            out[ev]["sb_ytd_country"] = best_ytd.get("country")

    return out


# =======================
# OW endpoints
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
    url = f"{FINA_BASE}/events/{event_id}"
    data = http_get_json(url, HEADERS_WA)

    heats = data.get("Heats", [])
    if not heats:
        return []

    results = heats[0].get("Results", [])
    out: List[Dict[str, Any]] = []

    for a in results:
        wa_id = a.get("PersonId") or a.get("AthleteId") or a.get("CompetitorId") or a.get("Id") or ""
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
# Profile + pool cached
# =======================
def _cache_path_profile(wa_id: str) -> str:
    return os.path.join(CACHE_PROFILE_DIR, f"{wa_id}.json")


def _cache_path_pool_rows(wa_id: str) -> str:
    return os.path.join(CACHE_POOL_DIR, f"{wa_id}.json")


def fetch_wa_profile(wa_id: str) -> Dict[str, Any]:
    if not wa_id:
        return {}

    with _PROFILE_LOCK:
        if wa_id in _WA_PROFILE_CACHE:
            return _WA_PROFILE_CACHE[wa_id]

    p = _cache_path_profile(wa_id)
    if _is_cache_fresh(p, CACHE_TTL_SECONDS):
        cached = _read_json_file(p)
        if isinstance(cached, dict):
            with _PROFILE_LOCK:
                _WA_PROFILE_CACHE[wa_id] = cached
            return cached

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

    prof: Dict[str, Any] = {}
    for url in candidates:
        try:
            js = http_get_json(url, HEADERS_WA)
            if isinstance(js, dict) and js:
                prof = js
                break
        except Exception:
            continue

    with _PROFILE_LOCK:
        _WA_PROFILE_CACHE[wa_id] = prof

    if prof:
        _write_json_file(p, prof)

    return prof


def fetch_wa_pool_best_attempt(wa_id: str, ow_date: date) -> Dict[str, Dict[str, Optional[str]]]:
    if not wa_id:
        return {}

    with _POOL_LOCK:
        if wa_id in _WA_POOL_ROWS_CACHE:
            rows = _WA_POOL_ROWS_CACHE[wa_id]
            return wa_compute_pool_bests(rows, ow_date) if rows else {}

    p = _cache_path_pool_rows(wa_id)
    if _is_cache_fresh(p, CACHE_TTL_SECONDS):
        cached_rows = _read_json_file(p)
        if isinstance(cached_rows, list):
            with _POOL_LOCK:
                _WA_POOL_ROWS_CACHE[wa_id] = cached_rows
            return wa_compute_pool_bests(cached_rows, ow_date) if cached_rows else {}

    candidates = [
        f"{FINA_BASE}/athletes/{wa_id}/results",
        f"{FINA_BASE}/persons/{wa_id}/results",
        f"{FINA_BASE}/person/{wa_id}/results",
        f"{FINA_BASE}/athlete/{wa_id}/results",
    ]

    js = None
    for url in candidates:
        try:
            js_try = http_get_json(url, HEADERS_WA)
            if js_try:
                js = js_try
                break
        except Exception:
            continue

    if not js:
        with _POOL_LOCK:
            _WA_POOL_ROWS_CACHE[wa_id] = []
        _write_json_file(p, [])
        return {}

    rows = wa_extract_pool_rows(js)

    with _POOL_LOCK:
        _WA_POOL_ROWS_CACHE[wa_id] = rows

    _write_json_file(p, rows)

    if not rows:
        return {}

    return wa_compute_pool_bests(rows, ow_date)


# =======================
# Read competitions from xlsx
# =======================
def load_competitions_from_xlsx() -> Tuple[List[str], str, Dict[str, Optional[str]]]:
    pattern = os.path.join(BASE_DIR, "competitions_*.xlsx")
    matches = sorted(glob.glob(pattern))

    if not matches:
        print(f"No file found in {BASE_DIR} matching competitions_*.xlsx")
        sys.exit(1)

    xlsx_path = matches[0]
    input_stem = os.path.splitext(os.path.basename(xlsx_path))[0]
    lg.info("Using competitions file: %s", os.path.basename(xlsx_path))

    df = pd.read_excel(xlsx_path)
    if df.empty:
        print(f"No competition IDs in {xlsx_path}")
        sys.exit(1)

    # id column
    col_id = None
    for c in df.columns:
        if str(c).strip().lower() == "id":
            col_id = c
            break
    if col_id is None:
        col_id = df.columns[0]

    # country column (best-effort)
    col_country = None
    for c in df.columns:
        lc = str(c).strip().lower()
        if lc in {"countrycode", "country_code", "country", "countryname", "country_name"}:
            col_country = c
            break

    comp_ids: List[str] = []
    comp_country: Dict[str, Optional[str]] = {}

    for _, row in df.iterrows():
        v_id = row.get(col_id)
        if pd.isna(v_id):
            continue
        cid = str(v_id).strip()
        if not cid:
            continue

        comp_ids.append(cid)

        cc = None
        if col_country is not None:
            v_cc = row.get(col_country)
            if v_cc is not None and not pd.isna(v_cc):
                cc = str(v_cc).strip().upper()
                cc = cc if cc else None

        comp_country[cid] = cc

    comp_ids = [x for i, x in enumerate(comp_ids) if x not in comp_ids[:i]]

    if not comp_ids:
        print(f"No competition IDs found in column '{col_id}' inside {xlsx_path}")
        sys.exit(1)

    return comp_ids, input_stem, comp_country


# =======================
# Athlete processing
# =======================
def process_athlete(a: Dict[str, Any], ev_name: str, ev_gender: str, ow_date: date,
                    comp_name: str, ow_country_code: Optional[str], ow_comp_country: Optional[str]
                    ) -> Tuple[str, List[Dict[str, Any]]]:

    wa_id = a.get("wa_id", "")
    full_name = a.get("full_name", "")
    nat = a.get("nat", "")
    ow_time = a.get("ow_time", "")
    ow_rank = a.get("ow_rank", "")

    if not ow_rank:
        return ev_gender, []

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

    if SLEEP_BETWEEN_ATHLETES:
        time.sleep(SLEEP_BETWEEN_ATHLETES)

    out_rows: List[Dict[str, Any]] = []
    for pool_event in POOL_EVENTS:
        wa_ev = wa_pool.get(pool_event, {}) if isinstance(wa_pool, dict) else {}

        # Pool meet country only (do not fallback to OW competition country)
        sb_cty = wa_ev.get("sb_ytd_country")
        pb_cty = wa_ev.get("pb_upto_country")

        out_rows.append({
            "Competition": comp_name,
            "Country": ow_country_code,
            # "OW_CompetitionCountry": ow_comp_country,
            "OW_Event": ev_name,
            "OW_Date": ow_date.isoformat(),

            # "WA_ID": wa_id,
            "Athlete": full_name,
            "NAT": nat,
            # "Sex": wa_sex,
            "Birth": wa_birth,

            "OW_Rank": ow_rank,
            "OW_Time": ow_time,

            "PoolEvent": pool_event,

            "WA_SB_YTD_Time": wa_ev.get("sb_ytd_time"),
            "WA_SB_YTD_Date": wa_ev.get("sb_ytd_date"),
            "WA_SB_Upto_Meet": wa_ev.get("sb_ytd_meet"),
            "WA_SB_Upto_Country": sb_cty,

            "WA_PB_Upto_Time": wa_ev.get("pb_upto_time"),
            "WA_PB_Upto_Date": wa_ev.get("pb_upto_date"),
            "WA_PB_Upto_Meet": wa_ev.get("pb_upto_meet"),
            "WA_PB_Upto_Country": pb_cty,
        })

    return ev_gender, out_rows


def save_csv(df: pd.DataFrame, path: str) -> None:
    df.to_csv(path, index=False, encoding="utf-8-sig")


# =======================
# Analyze one competition
# =======================
def analyze_race(
    competition_id: str,
    comp_country_map: Dict[str, Optional[str]]
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:

    ow_comp_country = comp_country_map.get(competition_id)

    meta = fetch_competition_meta(competition_id)
    comp_name = meta.get("Name", "")
    comp_from = parse_iso_date(meta.get("From", ""))
    comp_to = parse_iso_date(meta.get("To", ""))

    ow_country_code = meta.get("CountryCode") or ow_comp_country

    lg.info("Competition: %s (%s -> %s) [id=%s]", comp_name, comp_from, comp_to, competition_id)

    ow_events = fetch_ow_events(meta)
    picked = pick_10km_events(ow_events)
    if not picked:
        lg.warning("No 10km OW events found for competition_id=%s", competition_id)
        return [], [], []

    ow_date = comp_to or comp_from
    if not ow_date:
        lg.warning("Cannot determine OW date for competition_id=%s", competition_id)
        return [], [], []

    # Pre-fetch athletes per event to get correct totals for progress
    event_athletes: Dict[str, List[Dict[str, Any]]] = {}
    comp_total = 0

    for ev in picked:
        ev_id = ev["id"]
        ats = fetch_event_results(ev_id)
        ats = [a for a in ats if a.get("ow_rank")]

        if LIMIT_ATHLETES:
            ats = ats[:LIMIT_ATHLETES]

        event_athletes[ev_id] = ats
        comp_total += len(ats)

    comp_done = 0

    rows_all: List[Dict[str, Any]] = []
    rows_women: List[Dict[str, Any]] = []
    rows_men: List[Dict[str, Any]] = []

    for ev in picked:
        ev_name = ev["name"]
        ev_gender = ev.get("gender", "")
        ev_id = ev["id"]

        athletes = event_athletes.get(ev_id, [])
        sec_total = len(athletes)
        sec_done = 0

        lg.info("OW event: %s (%s) id=%s", ev_name, ev_gender, ev_id)
        lg.info("Athletes: %d", sec_total)

        # Reserve 2 lines for progress bar
        print("\n\n", end="")
        _print_progress(f"Section: {ev_gender}", 0, sec_total, comp_done, comp_total)

        futures = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            for a in athletes:
                futures.append(
                    ex.submit(
                        process_athlete,
                        a, ev_name, ev_gender, ow_date,
                        comp_name, ow_country_code, ow_comp_country
                    )
                )

            last_print = time.time()
            for fut in as_completed(futures):
                try:
                    g, part = fut.result()
                    if part:
                        rows_all.extend(part)
                        if str(g).lower().startswith("w"):
                            rows_women.extend(part)
                        elif str(g).lower().startswith("m"):
                            rows_men.extend(part)
                except Exception as e:
                    lg.warning("Athlete task failed: %s", e)

                sec_done += 1
                comp_done += 1

                now = time.time()
                if now - last_print > 0.1 or sec_done == sec_total:
                    _print_progress(f"Section: {ev_gender}", sec_done, sec_total, comp_done, comp_total)
                    last_print = now

        print()

    return rows_all, rows_women, rows_men


# =======================
# Main
# =======================
def main() -> None:
    comp_ids, input_stem, comp_country_map = load_competitions_from_xlsx()
    lg.info("Competitions to analyze: %d", len(comp_ids))

    rows_all: List[Dict[str, Any]] = []

    for idx, cid in enumerate(comp_ids, 1):
        lg.info("==== [%d/%d] Analyzing competition_id=%s ====", idx, len(comp_ids), cid)
        ra, _, _ = analyze_race(cid, comp_country_map)
        rows_all.extend(ra)

    if not rows_all:
        lg.error("No data to save.")
        return

    df_all = pd.DataFrame(rows_all)

    out_csv = os.path.join(BASE_DIR, f"ow_pool_join_{input_stem}.csv")
    save_csv(df_all, out_csv)
    lg.info("Saved: %s", out_csv)

    if WRITE_XLSX:
        out_xlsx = os.path.join(BASE_DIR, f"ow_pool_join_{input_stem}.xlsx")
        df_all.to_excel(out_xlsx, index=False)
        lg.info("Saved: %s", out_xlsx)


if __name__ == "__main__":
    try:
        main()
    finally:
        if CLEANUP_CACHE_AT_END:
            lg.info("Cleaning cache folders...")
            cleanup_cache_dirs()
