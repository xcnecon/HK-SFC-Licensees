#!/usr/bin/env python3
"""
SFC Licensee Database Updater

Builds and maintains a local SQLite database of all Hong Kong Securities and
Futures Commission (SFC) licensed corporations and individuals — both active
and historical — by scraping the SFC public register at
https://apps.sfc.hk/publicregWeb (no authentication required).

Usage:
    python update_sfc.py [--full] [--db PATH]

    --full      Force complete re-fetch, ignoring sfc_upd timestamps.
    --db PATH   Path to the SQLite database file (default: ./sfc.db).

The database is created from schema.sql (must sit next to this script) on
first run. Subsequent runs skip entities updated within the last 12 hours.

To route requests through an HTTP/HTTPS proxy, set the standard environment
variables HTTPS_PROXY / HTTP_PROXY before running. The `requests` library
honours these automatically.
"""

import argparse
import calendar
import json
import os
import re
import sqlite3
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import requests
from requests.adapters import HTTPAdapter

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_URL = "https://apps.sfc.hk/publicregWeb"
SEARCH_URL = f"{BASE_URL}/searchByRaJson"
REQUEST_DELAY = 0            # seconds between requests (0 = no throttle)
MAX_RETRIES = 3
RETRY_BACKOFF = 2.0          # exponential backoff multiplier
WORKER_THREADS = 8           # parallel HTTP fetching threads
HOURS_THRESHOLD = 12         # skip re-fetch if updated within this many hours
ACTIVITY_TYPES = list(range(1, 14))
LETTERS_CORP = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
LETTERS_INDI = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SCHEMA_PATH = os.path.join(_SCRIPT_DIR, "schema.sql")
# DB_PATH defaults to ./sfc.db in the current working directory.
# Override via the --db CLI flag or the SFC_DB_PATH environment variable.
DB_PATH = os.environ.get("SFC_DB_PATH") or os.path.join(os.getcwd(), "sfc.db")

# Pre-SFC-system date: licences with this start date are treated as NULL
PRE_SYSTEM_DATE = "2003-04-01"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def log(msg):
    """Print progress message to stderr."""
    print(msg, file=sys.stderr, flush=True)

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
_session = requests.Session()
_session.headers.update({
    "User-Agent": "sfc-updater/1.0 (+https://github.com/)",
    "Accept": "text/html,application/json,*/*",
})
# requests honours HTTPS_PROXY / HTTP_PROXY env vars automatically; no
# explicit proxy configuration is needed here.
_pool_adapter = HTTPAdapter(pool_connections=WORKER_THREADS, pool_maxsize=WORKER_THREADS)
_session.mount("https://", _pool_adapter)
_session.mount("http://", _pool_adapter)

_last_request_time = 0.0

def _throttle():
    """Ensure at least REQUEST_DELAY seconds between HTTP requests."""
    global _last_request_time
    elapsed = time.time() - _last_request_time
    if elapsed < REQUEST_DELAY:
        time.sleep(REQUEST_DELAY - elapsed)
    _last_request_time = time.time()

def fetch_get(url):
    """GET with retry and exponential backoff. Returns response text or ''."""
    for attempt in range(MAX_RETRIES):
        try:
            _throttle()
            resp = _session.get(url, timeout=30)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_BACKOFF ** (attempt + 1)
                log(f"  Retry {attempt+1}/{MAX_RETRIES} for GET {url}: {e} (wait {wait:.1f}s)")
                time.sleep(wait)
            else:
                log(f"  FAILED GET after {MAX_RETRIES} attempts: {url} -> {e}")
                return ""

def fetch_post(url, data):
    """POST with retry and exponential backoff. Returns response text or ''."""
    for attempt in range(MAX_RETRIES):
        try:
            _throttle()
            resp = _session.post(
                url, data=data, timeout=30,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_BACKOFF ** (attempt + 1)
                log(f"  Retry {attempt+1}/{MAX_RETRIES} for POST: {e} (wait {wait:.1f}s)")
                time.sleep(wait)
            else:
                log(f"  FAILED POST after {MAX_RETRIES} attempts: {url} -> {e}")
                return ""

def get_sfc_page(page, sfc_id, ptype):
    """Fetch an SFC entity page. ptype: 'corp', 'ri', or 'indi'."""
    return fetch_get(f"{BASE_URL}/{ptype}/{sfc_id}/{page}")

# ---------------------------------------------------------------------------
# JSON-from-HTML extraction
# ---------------------------------------------------------------------------
def extract_json_var(html, var_name):
    """Extract a JSON array/object assigned to a JS variable in HTML.

    Uses balanced bracket counting for robustness.
    Looks for: var rorawData = [...]; or rorawData = [...];
    Returns parsed Python object, or None if not found.
    """
    if not html:
        return None
    # Find the variable assignment
    pattern = re.compile(r'(?:var\s+)?' + re.escape(var_name) + r'\s*=\s*')
    m = pattern.search(html)
    if not m:
        return None
    pos = m.end()
    # Skip whitespace
    while pos < len(html) and html[pos] in ' \t\n\r':
        pos += 1
    if pos >= len(html):
        return None
    start_char = html[pos]
    if start_char not in '[{':
        return None
    end_char = ']' if start_char == '[' else '}'
    # Count balanced brackets, respecting strings
    depth = 0
    in_string = False
    i = pos
    while i < len(html):
        c = html[i]
        if in_string:
            if c == '\\':
                i += 2  # skip escaped char
                continue
            if c == '"':
                in_string = False
        else:
            if c == '"':
                in_string = True
            elif c == start_char:
                depth += 1
            elif c == end_char:
                depth -= 1
                if depth == 0:
                    raw = html[pos:i + 1]
                    try:
                        return json.loads(raw)
                    except json.JSONDecodeError:
                        # Try stripping trailing commas before ] or }
                        raw2 = re.sub(r',\s*([}\]])', r'\1', raw)
                        try:
                            return json.loads(raw2)
                        except json.JSONDecodeError:
                            log(f"  WARNING: Could not parse JSON for var {var_name}")
                            return None
        i += 1
    return None

def extract_search_items(response_text):
    """Extract items array from search API JSON response."""
    if not response_text:
        return []
    try:
        data = json.loads(response_text)
        return data.get("items", [])
    except json.JSONDecodeError:
        log("  WARNING: Could not parse search response JSON")
        return []

# ---------------------------------------------------------------------------
# Name helpers
# ---------------------------------------------------------------------------
def strip_space(s):
    """Remove multiple spaces and trim."""
    if not s:
        return s
    s = s.strip()
    while "  " in s:
        s = s.replace("  ", " ")
    return s

def title_case_word(word, is_surname=False):
    """Convert a single word (possibly hyphenated) to proper case."""
    if not word or word.startswith("("):
        return word
    parts = word.split("-")
    cased = []
    for part in parts:
        if not part:
            cased.append(part)
            continue
        # Base: first letter upper, rest lower
        p = part[0].upper() + part[1:].lower() if len(part) > 1 else part.upper()
        # Handle O', D' prefix
        if len(p) > 2 and p[1] == "'" and p[0] in "OD":
            p = p[0].upper() + "'" + p[2].upper() + (p[3:] if len(p) > 3 else "")
        # Mc prefix
        elif p.startswith("Mc") and len(p) > 2:
            p = "Mc" + p[2].upper() + (p[3:] if len(p) > 3 else "")
        # Mac/Fitz for surnames only
        elif is_surname:
            for pfx in ("Mac", "Fitz"):
                if p.lower().startswith(pfx.lower()) and len(p) > len(pfx):
                    rest = p[len(pfx):]
                    p = pfx + rest[0].upper() + rest[1:]
                    break
        # Roman numerals
        if p.upper() in ("II", "III", "IV", "VI", "VII", "VIII", "IX"):
            p = p.upper()
        # Lowercase connectors
        elif p.lower() in ("de", "da", "of", "la", "von", "van", "den", "der"):
            p = p.lower()
        # Initials like A.B.C
        elif len(p) > 1 and p[1] == ".":
            p = p.upper()
        cased.append(p)
    return "-".join(cased)

def title_case_name(s, is_surname=False):
    """Convert a name string to proper case."""
    if not s:
        return s
    s = s.replace("\u0060", "'").replace("\u2019", "'")
    s = strip_space(s)
    words = s.split()
    result = []
    for word in words:
        result.append(title_case_word(word, is_surname))
    return " ".join(result)

def name_split(full_name):
    """Split an SFC full name into (surname, forenames).

    SFC names have uppercase surname followed by forenames.
    "CHAN Wing Hing Barry" -> ("Chan", "Wing Hing Barry")
    "LI Ngok" -> ("Li", "Ngok")
    """
    n = strip_space(full_name)
    if not n:
        return ("", "")

    p = n.find(" ")
    if p == -1:
        # Single word, no forenames
        return (title_case_name(n, True), "")

    length = len(n)
    prevspace = p
    x = p + 1
    while x < length:
        ch = n[x]
        code = ord(ch)
        # Lowercase letter or period after initial -> start of forenames
        if code >= ord('a') or ch == '.':
            break
        # Hyphen after single letter+space -> forename like I-Chen
        if ch == '-' and x == prevspace + 2:
            break
        if ch == ' ':
            prevspace = x
        # Single letter surrounded by spaces -> forename initial
        snippet = n[x - 1:x + 2] if x + 1 < length else n[x - 1:x + 1] + " "
        if len(snippet.strip()) == 1:
            x += 1
            break
        x += 1

    if x >= length:
        # Possibly all caps, or surname with initials
        n1 = n[:p].strip()
        n2 = n[p + 1:].strip()
    else:
        n1 = n[:x - 1].strip()
        n2 = n[x - 1:].strip()

    # Convert to proper case
    n1 = title_case_name(n1, True)
    if n2:
        n2 = title_case_name(n2, False)
        # Handle commas
        comma_count = n2.count(",")
        if comma_count == 1:
            cp = n2.index(",")
            eng_name = n2[cp + 1:].strip()
            n2 = eng_name + " " + n2[:cp].strip()
        else:
            n2 = n2.replace(",", " ")
            n2 = strip_space(n2)
    if n2 == "-":
        n2 = ""

    return (n1, n2)

# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------
def parse_date(d):
    """Parse a date string from SFC JSON. Returns ISO date string or None."""
    if not d or d == "null" or d == "None" or str(d).strip() == "":
        return None
    d = str(d).strip()
    for fmt in (
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%Y/%m/%d",
        "%b %d, %Y %I:%M:%S %p",    # "Nov 14, 2019 12:00:00 AM"
        "%b %d, %Y",                  # "Nov 14, 2019"
        "%B %d, %Y %I:%M:%S %p",     # "November 14, 2019 12:00:00 AM"
        "%B %d, %Y",                  # "November 14, 2019"
    ):
        try:
            return datetime.strptime(d, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None

def parse_start_date(d):
    """Parse start date: pre-system date (2003-04-01) -> None."""
    iso = parse_date(d)
    if iso == PRE_SYSTEM_DATE:
        return None
    return iso

def parse_end_date(d):
    """Parse end date: null/missing -> None (ongoing)."""
    if not d or d == "null" or d == "None" or str(d).strip() == "":
        return None
    return parse_date(d)

def last_day_of_month(year, month):
    """Return YYYY-MM-DD of last day of given month."""
    day = calendar.monthrange(year, month)[1]
    return f"{year:04d}-{month:02d}-{day:02d}"

def now_iso():
    """Return current datetime in ISO 8601."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def hours_since(timestamp_str):
    """Return hours elapsed since the given ISO timestamp."""
    if not timestamp_str:
        return 999999
    try:
        dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        try:
            dt = datetime.strptime(timestamp_str, "%Y-%m-%d")
        except ValueError:
            return 999999
    return (datetime.now() - dt).total_seconds() / 3600

# ---------------------------------------------------------------------------
# Database setup and helpers
# ---------------------------------------------------------------------------
_db = None
_db_lock = threading.Lock()

def get_db():
    """Get or create the database connection.

    Must be called from the main thread before spawning workers.
    """
    global _db
    if _db is None:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        need_schema = not os.path.exists(DB_PATH)
        _db = sqlite3.connect(DB_PATH, check_same_thread=False)
        _db.execute("PRAGMA journal_mode=WAL")
        _db.execute("PRAGMA foreign_keys=ON")
        _db.row_factory = sqlite3.Row
        if need_schema:
            log(f"Creating new database at {DB_PATH}")
            with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
                _db.executescript(f.read())
            _db.commit()
    return _db

def db_execute(sql, params=None):
    with _db_lock:
        db = get_db()
        return db.execute(sql, params) if params else db.execute(sql)

def db_commit():
    with _db_lock:
        get_db().commit()

def db_fetchone(sql, params=None):
    with _db_lock:
        db = get_db()
        cur = db.execute(sql, params) if params else db.execute(sql)
        return cur.fetchone()

def db_fetchall(sql, params=None):
    with _db_lock:
        db = get_db()
        cur = db.execute(sql, params) if params else db.execute(sql)
        return cur.fetchall()

def db_exists(sql, params=None):
    row = db_fetchone(sql, params)
    return row is not None and row[0]

def ensure_activity_type(act_type):
    """Ensure an activity type exists in the activity table.
    Auto-inserts unknown types so foreign key constraints don't fail.
    """
    row = db_fetchone("SELECT id FROM activity WHERE id=?", (act_type,))
    if not row:
        log(f"  New activity type discovered: {act_type}")
        db_execute(
            "INSERT OR IGNORE INTO activity (id, name) VALUES (?, ?)",
            (act_type, f"Unknown activity type {act_type}"),
        )
        db_commit()

# ---------------------------------------------------------------------------
# Cleaning helpers
# ---------------------------------------------------------------------------
def clean_org_name(name):
    """Clean an organisation name."""
    if not name:
        return name
    # Strip "trading as" suffix
    idx = name.find("(trading as")
    if idx != -1:
        name = name[:idx].strip()
    name = name.replace("&amp;", "&")
    name = name.replace("\\u0026", "&")
    name = name.replace("\\u0027", "'")
    # Move "The " prefix to suffix
    if name.startswith("The "):
        name = name[4:] + " (The)"
    return name

def clean_chinese_name(cn):
    """Clean a Chinese name field."""
    if not cn or cn in ("null", "\\u0000", None):
        return ""
    return cn.replace(" ", "")

def org_type_from_name(name):
    """Infer org_type from name suffix."""
    if not name:
        return None
    if name.endswith(" Limited") or name.endswith(" Ltd") or name.endswith(" Ltd."):
        return 19
    if name.endswith(" LLC") or name.endswith(" L.L.C."):
        return 10
    if name.endswith(" LLP") or name.endswith(" L.L.P."):
        return 9
    return None

# ---------------------------------------------------------------------------
# Entity resolution: organisations
# ---------------------------------------------------------------------------
def sfc_id_to_org_id(sfc_id, name, c_name, ptype):
    """Resolve an SFC CE reference to an internal org ID.

    Creates a new org if necessary. Handles amalgamations via old_sfc_ids.
    Thread-safe: retries once on IntegrityError from concurrent inserts.
    """
    try:
        return _sfc_id_to_org_id_impl(sfc_id, name, c_name, ptype)
    except sqlite3.IntegrityError:
        return _sfc_id_to_org_id_impl(sfc_id, name, c_name, ptype)

def _sfc_id_to_org_id_impl(sfc_id, name, c_name, ptype):
    # 1. Direct lookup by sfc_id
    row = db_fetchone("SELECT person_id FROM organisations WHERE sfc_id=?", (sfc_id,))
    if row:
        return row[0]

    # 2. Check old_sfc_ids (amalgamations)
    row = db_fetchone("SELECT org_id FROM old_sfc_ids WHERE sfc_id=?", (sfc_id,))
    if row:
        return row[0]

    # 3. Not found — try to match by name or create new
    name = clean_org_name(name)
    c_name = clean_chinese_name(c_name)
    otype = org_type_from_name(name)

    # Try to find by name (case-insensitive, prefer non-dissolved)
    row = db_fetchone(
        "SELECT person_id, sfc_id, org_type, c_name FROM organisations "
        "WHERE dis_date IS NULL AND name1=? COLLATE NOCASE ORDER BY domicile",
        (name,),
    )
    if row:
        org_id = row["person_id"]
        existing_sfc_id = row["sfc_id"]
        if existing_sfc_id and existing_sfc_id != sfc_id:
            # Different SFC ID -> amalgamation. Record the old one.
            amal_date = None
            html = get_sfc_page("licences", sfc_id, ptype)
            if html:
                m = re.search(r'amalgamated on\s+(\d{2}/\d{2}/\d{4})', html)
                if m:
                    amal_date = parse_date(m.group(1))
            try:
                db_execute(
                    "INSERT OR IGNORE INTO old_sfc_ids (sfc_id, until_date, org_id) VALUES (?,?,?)",
                    (existing_sfc_id, amal_date, org_id),
                )
            except sqlite3.IntegrityError:
                pass
            log(f"  Amalgamation: old {existing_sfc_id} -> new {sfc_id} for org {org_id}")

        # Update the SFC ID and fill in missing fields
        db_execute("UPDATE organisations SET sfc_id=? WHERE person_id=?", (sfc_id, org_id))
        if otype and not row["org_type"]:
            db_execute("UPDATE organisations SET org_type=? WHERE person_id=?", (otype, org_id))
        if c_name and not row["c_name"]:
            db_execute("UPDATE organisations SET c_name=? WHERE person_id=?", (c_name, org_id))
        db_commit()
        return org_id

    # 4. Not found at all — create new org
    cur = db_execute(
        "INSERT INTO organisations (sfc_id, name1, c_name, org_type) VALUES (?,?,?,?)",
        (sfc_id, name, c_name if c_name else None, otype),
    )
    org_id = cur.lastrowid
    db_commit()
    log(f"  Created new org: {org_id} {sfc_id} {name}")
    return org_id

# ---------------------------------------------------------------------------
# Entity resolution: people
# ---------------------------------------------------------------------------
def get_or_create_person(sfc_id, n1, n2, c_name):
    """Find or create a person by SFC ID.

    Returns person_id.
    Thread-safe: retries once on IntegrityError from concurrent inserts.
    """
    try:
        return _get_or_create_person_impl(sfc_id, n1, n2, c_name)
    except sqlite3.IntegrityError:
        return _get_or_create_person_impl(sfc_id, n1, n2, c_name)

def _get_or_create_person_impl(sfc_id, n1, n2, c_name):
    c_name = clean_chinese_name(c_name)

    # 1. Direct SFC ID lookup
    row = db_fetchone("SELECT person_id, c_name FROM people WHERE sfc_id=?", (sfc_id,))
    if row:
        pid = row["person_id"]
        if c_name and not row["c_name"]:
            db_execute("UPDATE people SET c_name=? WHERE person_id=?", (c_name, pid))
            db_commit()
        return pid

    # 2. Try to find by exact name
    if n2:
        row = db_fetchone(
            "SELECT person_id, sfc_id FROM people WHERE name1=? AND name2=?",
            (n1, n2),
        )
    else:
        row = db_fetchone(
            "SELECT person_id, sfc_id FROM people WHERE name1=? AND name2 IS NULL",
            (n1,),
        )

    if row:
        if not row["sfc_id"]:
            # Found person without SFC ID — assign it
            db_execute("UPDATE people SET sfc_id=? WHERE person_id=?", (sfc_id, row["person_id"]))
            if c_name:
                db_execute(
                    "UPDATE people SET c_name=? WHERE person_id=? AND c_name IS NULL",
                    (c_name, row["person_id"]),
                )
            db_commit()
            return row["person_id"]
        else:
            # Name clash with different SFC ID — create new with extension
            n2_ext = f"{n2} (SFC:{sfc_id})".strip() if n2 else f"(SFC:{sfc_id})"
            cur = db_execute(
                "INSERT INTO people (sfc_id, name1, name2, c_name) VALUES (?,?,?,?)",
                (sfc_id, n1, n2_ext, c_name or None),
            )
            db_commit()
            log(f"  NEW person (name clash): {cur.lastrowid} {sfc_id} {n1}, {n2_ext}")
            return cur.lastrowid

    # 3. No exact match — check for case-insensitive clash
    if n2:
        clash = db_exists(
            "SELECT 1 FROM people WHERE name1=? COLLATE NOCASE AND name2=? COLLATE NOCASE",
            (n1, n2),
        )
    else:
        clash = db_exists(
            "SELECT 1 FROM people WHERE name1=? COLLATE NOCASE AND name2 IS NULL",
            (n1,),
        )
    if clash:
        n2_final = f"{n2} (SFC:{sfc_id})".strip() if n2 else f"(SFC:{sfc_id})"
    else:
        n2_final = n2 if n2 else None

    cur = db_execute(
        "INSERT INTO people (sfc_id, name1, name2, c_name) VALUES (?,?,?,?)",
        (sfc_id, n1, n2_final, c_name or None),
    )
    db_commit()
    log(f"  NEW person: {cur.lastrowid} {sfc_id} {n1}, {n2_final}")
    return cur.lastrowid

# ---------------------------------------------------------------------------
# Reading SFC entity pages
# ---------------------------------------------------------------------------
def read_sfc_people(html, page_type):
    """Extract list of people (ROs or Reps) from an entity HTML page.

    page_type: 'ro' or 'rep'
    Returns list of dicts with keys: sfc_id, name1, name2, c_name
    """
    var_name = f"{page_type}rawData"
    data = extract_json_var(html, var_name)
    if not data or not isinstance(data, list):
        return []

    people = []
    for item in data:
        # SFC uses both "ceref" and "ceRef" across different pages
        ceref = item.get("ceref") or item.get("ceRef", "")
        if not ceref:
            continue
        full_name = strip_space(item.get("fullName", ""))
        full_name = full_name.replace("\\u0027", "'")
        n1, n2 = name_split(full_name)
        c_name = clean_chinese_name(item.get("entityNameChi", ""))
        people.append({"sfc_id": ceref, "name1": n1, "name2": n2, "c_name": c_name})
    return people

def read_ind_lic_records(sfc_id):
    """Read the licence history of an individual from SFC.

    Returns (records, found) where records is a list of dicts.
    """
    html = get_sfc_page("licenceRecord", sfc_id, "indi")
    if not html:
        return [], False

    data = extract_json_var(html, "licRecordData")
    if data is None:
        return [], False
    if isinstance(data, list) and len(data) == 0:
        # SFC returned empty array — don't delete our records
        return [], False

    records = []
    for item in data:
        role = 1 if item.get("lcRole") == "RO" else 0
        principal = item.get("prinCeName", "")
        c_principal = item.get("prinCeNameChin", "")
        if c_principal in ("null", "\\u0000", None):
            c_principal = ""
        org_ce = item.get("prinCeRef", "")

        reg_act = item.get("regulatedActivity", {})
        act_type = reg_act.get("actType") if isinstance(reg_act, dict) else None
        if act_type is None:
            continue
        act_type = int(act_type)

        periods = item.get("effectivePeriodList", [])
        if not isinstance(periods, list):
            continue
        for period in periods:
            started = parse_start_date(period.get("effectiveDate"))
            ended = parse_end_date(period.get("endDate"))
            records.append({
                "role": role,
                "start_date": started,
                "end_date": ended,
                "principal": principal,
                "org_ce": org_ce,
                "c_principal": c_principal,
                "act_type": act_type,
            })
    return records, True

def read_org_lic_records(sfc_id, sfc_ri):
    """Read the licence history of an organisation.

    Returns (records, found, sfc_ri_updated).
    """
    # Try expected type first, fall back to the other
    if sfc_ri:
        html = get_sfc_page("licences", sfc_id, "ri")
        if not html or "No record found" in html:
            html = get_sfc_page("licences", sfc_id, "corp")
            if html and "No record found" not in html:
                sfc_ri = False
    else:
        html = get_sfc_page("licences", sfc_id, "corp")
        if not html or "No record found" in html:
            html = get_sfc_page("licences", sfc_id, "ri")
            if html and "No record found" not in html:
                sfc_ri = True

    if not html:
        return [], False, sfc_ri

    data = extract_json_var(html, "licRecordData")
    if data is None or not isinstance(data, list) or len(data) == 0:
        return [], False, sfc_ri

    records = []
    for item in data:
        lc_type = item.get("lcType", "")
        ri = 1 if lc_type == "E" else 0

        reg_act = item.get("regulatedActivity", {})
        act_type = reg_act.get("actType") if isinstance(reg_act, dict) else None
        if act_type is None:
            continue
        act_type = int(act_type)

        periods = item.get("effectivePeriodList", [])
        if not isinstance(periods, list):
            continue
        for period in periods:
            started = parse_start_date(period.get("effectiveDate"))
            ended = parse_end_date(period.get("endDate"))
            records.append({
                "ri": ri,
                "act_type": act_type,
                "start_date": started,
                "end_date": ended,
            })
    return records, True, sfc_ri

# ---------------------------------------------------------------------------
# Updating individual licence records in DB
# ---------------------------------------------------------------------------
def update_ind_lic_rec(staff_id, records):
    """Update the detailed licence history of an individual.

    Returns (changed, records_with_org_ids).
    """
    changed = False
    last_sfc_id = ""
    org_id = None

    for rec in records:
        role = rec["role"]
        started = rec["start_date"]
        ended = rec["end_date"]
        act_type = rec["act_type"]
        org_sfc_id = rec["org_ce"]

        # Resolve org (cache consecutive lookups for same org)
        if org_sfc_id != last_sfc_id:
            org_id = sfc_id_to_org_id(org_sfc_id, rec["principal"], rec["c_principal"], "corp")
        last_sfc_id = org_sfc_id
        rec["org_id"] = org_id

        ensure_activity_type(act_type)

        # Check if record exists
        if started is None:
            row = db_fetchone(
                "SELECT id, end_date FROM licrec "
                "WHERE staff_id=? AND org_id=? AND role=? AND act_type=? AND start_date IS NULL",
                (staff_id, org_id, role, act_type),
            )
        else:
            row = db_fetchone(
                "SELECT id, end_date FROM licrec "
                "WHERE staff_id=? AND org_id=? AND role=? AND act_type=? AND start_date=?",
                (staff_id, org_id, role, act_type, started),
            )

        if row is None:
            # New record
            changed = True
            db_execute(
                "INSERT INTO licrec (staff_id, org_id, role, act_type, start_date, end_date) "
                "VALUES (?,?,?,?,?,?)",
                (staff_id, org_id, role, act_type, started, ended),
            )
        else:
            # Check for end_date changes
            rec_id = row["id"]
            existing_end = row["end_date"]
            if existing_end is None and ended is not None:
                db_execute("UPDATE licrec SET end_date=? WHERE id=?", (ended, rec_id))
                changed = True
            elif existing_end is not None and ended is None:
                db_execute("UPDATE licrec SET end_date=NULL WHERE id=?", (rec_id,))
                changed = True
            elif existing_end is not None and ended is not None and ended != existing_end:
                db_execute("UPDATE licrec SET end_date=? WHERE id=?", (ended, rec_id))
                changed = True

    # Clean up duplicates: two records with same open end or containment
    dups = db_fetchall(
        "SELECT DISTINCT L2.id FROM licrec L1 JOIN licrec L2 ON L1.id > L2.id "
        "AND L1.staff_id=? AND L2.staff_id=? "
        "AND L1.org_id=L2.org_id AND L1.act_type=L2.act_type AND L1.role=L2.role "
        "AND ((L1.end_date IS NULL AND L2.end_date IS NULL) "
        " OR (L1.start_date > L2.start_date AND L2.end_date IS NULL AND L1.end_date IS NOT NULL) "
        " OR (L1.start_date <= L2.start_date AND L1.end_date > L2.start_date))",
        (staff_id, staff_id),
    )
    for dup in dups:
        changed = True
        db_execute("DELETE FROM licrec WHERE id=?", (dup["id"],))

    # Clean up overlapping periods
    dups2 = db_fetchall(
        "SELECT DISTINCT L1.id FROM licrec L1 JOIN licrec L2 ON L1.id > L2.id "
        "AND L1.staff_id=? AND L2.staff_id=? "
        "AND L1.org_id=L2.org_id AND L1.act_type=L2.act_type AND L1.role=L2.role "
        "AND L1.start_date > L2.start_date AND L1.start_date < L2.end_date",
        (staff_id, staff_id),
    )
    for dup in dups2:
        changed = True
        db_execute("DELETE FROM licrec WHERE id=?", (dup["id"],))

    # Update SFC update timestamp
    db_execute("UPDATE people SET sfc_upd=? WHERE person_id=?", (now_iso(), staff_id))
    db_commit()
    return changed, records

# ---------------------------------------------------------------------------
# Updating org licence records in DB
# ---------------------------------------------------------------------------
def update_org_lic_rec(org_id, records, sfc_ri_new, sfc_ri_old):
    """Update the SFC licence history of one org."""
    # Update sfc_ri if changed
    if sfc_ri_new != sfc_ri_old:
        db_execute(
            "UPDATE organisations SET sfc_ri=? WHERE person_id=?",
            (1 if sfc_ri_new else 0, org_id),
        )

    for rec in records:
        ri = rec["ri"]
        act_type = rec["act_type"]
        started = rec["start_date"]
        ended = rec["end_date"]

        ensure_activity_type(act_type)

        if started is None:
            existing = db_fetchone(
                "SELECT id, end_date FROM olicrec WHERE org_id=? AND act_type=? AND start_date IS NULL",
                (org_id, act_type),
            )
        else:
            existing = db_fetchone(
                "SELECT id, end_date FROM olicrec WHERE org_id=? AND act_type=? AND start_date=?",
                (org_id, act_type, started),
            )

        if existing is None:
            db_execute(
                "INSERT INTO olicrec (org_id, ri, act_type, start_date, end_date) VALUES (?,?,?,?,?)",
                (org_id, ri, act_type, started, ended),
            )
        else:
            rec_id = existing["id"]
            ex_end = existing["end_date"]
            if ex_end is None and ended is not None:
                db_execute("UPDATE olicrec SET end_date=? WHERE id=?", (ended, rec_id))
            elif ex_end is not None and ended is None:
                db_execute("UPDATE olicrec SET end_date=NULL WHERE id=?", (rec_id,))

    db_commit()

# ---------------------------------------------------------------------------
# Directorship computation: period merging and RO-trumps-Rep
# ---------------------------------------------------------------------------
def _start_sort_key(period):
    """Sort key for periods: None start = -infinity (earliest)."""
    s = period[0]
    return (0, '') if s is None else (1, s)

def merge_periods(periods):
    """Merge a list of (start, end) periods into non-overlapping periods.

    None start = predates system (earliest).
    None end = ongoing (latest).
    """
    if not periods:
        return []
    sorted_p = sorted(periods, key=_start_sort_key)
    merged = [list(sorted_p[0])]
    for start, end in sorted_p[1:]:
        prev_start, prev_end = merged[-1]
        if prev_end is None:
            # Previous period extends to infinity — absorbs everything
            continue
        # Check overlap: new period starts before or when previous ends
        if start is None or start <= prev_end:
            # Extend end if needed
            if end is None:
                merged[-1][1] = None
            elif end > prev_end:
                merged[-1][1] = end
        else:
            merged.append([start, end])
    return [tuple(p) for p in merged]

def subtract_ro_from_rep(ro_periods, rep_periods):
    """Subtract RO periods from Rep periods (RO trumps Rep).

    Both inputs should be sorted, non-overlapping.
    Returns remaining Rep fragments.
    """
    if not rep_periods:
        return []
    if not ro_periods:
        return list(rep_periods)

    result = []
    # Work with mutable copies
    reps = [[s, e] for s, e in rep_periods]
    ro_idx = 0
    rep_idx = 0

    while ro_idx < len(ro_periods) and rep_idx < len(reps):
        rep_s, rep_e = reps[rep_idx]
        ro_s, ro_e = ro_periods[ro_idx]

        # Case 1: Both have null start
        if rep_s is None and ro_s is None:
            if ro_e is not None and (rep_e is None or rep_e > ro_e):
                # Rep extends beyond RO: shift rep start to RO end
                reps[rep_idx][0] = ro_e
                ro_idx += 1
            else:
                # Rep ends before or with RO: skip rep
                rep_idx += 1

        # Case 2: Rep starts before RO (rep_s is None or rep_s < ro_s)
        elif _date_lt(rep_s, ro_s):
            if rep_e is not None and _date_le(rep_e, ro_s):
                # Rep entirely before RO: keep it
                result.append((rep_s, rep_e))
                rep_idx += 1
            else:
                # Rep overlaps RO at start: end rep when RO starts, shift remainder
                result.append((rep_s, ro_s))
                reps[rep_idx][0] = ro_s

        # Case 3: Rep starts during RO (ro_e is None or rep_s <= ro_e)
        elif ro_e is None or _date_le(rep_s, ro_e):
            if ro_e is not None and (rep_e is None or rep_e > ro_e):
                # Rep extends beyond RO: shift rep start to RO end
                reps[rep_idx][0] = ro_e
                ro_idx += 1
            else:
                # Rep contained by RO: skip rep
                rep_idx += 1

        # Case 4: Rep starts after RO ends
        else:
            ro_idx += 1

    # Insert remaining Rep periods
    for i in range(rep_idx, len(reps)):
        result.append(tuple(reps[i]))

    return result

def _date_lt(a, b):
    """Is date a < date b? None = -infinity (earliest)."""
    if a is None and b is None:
        return False
    if a is None:
        return True   # -inf < anything
    if b is None:
        return False  # anything >= -inf
    return a < b

def _date_le(a, b):
    """Is date a <= date b? None = -infinity (earliest)."""
    if a is None:
        return True
    if b is None:
        return False
    return a <= b

def compute_directorships(person_id, lic_records):
    """Compute directorship periods from licence records.

    Steps:
    1. Group by (org_id, role) and merge overlapping periods
    2. For each org, subtract RO from Rep (RO trumps Rep)
    3. Write final periods to directorships table
    """
    # Group by (org_id, role)
    groups = defaultdict(list)
    for rec in lic_records:
        if "org_id" not in rec:
            continue
        key = (rec["org_id"], rec["role"])
        groups[key].append((rec["start_date"], rec["end_date"]))

    # Merge overlapping periods within each group
    merged = {}
    for (org_id, role), periods in groups.items():
        merged[(org_id, role)] = merge_periods(periods)

    # For each org, compute final periods (RO trumps Rep)
    all_orgs = set(org_id for (org_id, _) in merged)
    final = []  # (org_id, position_id, start, end)

    for org_id in all_orgs:
        ro_periods = merged.get((org_id, 1), [])
        rep_periods = merged.get((org_id, 0), [])

        # RO periods become position_id=395
        for s, e in ro_periods:
            final.append((org_id, 395, s, e))

        # Subtract RO from Rep, remaining become position_id=394
        remaining_rep = subtract_ro_from_rep(ro_periods, rep_periods)
        for s, e in remaining_rep:
            final.append((org_id, 394, s, e))

    # Delete existing RO/Rep directorships for this person
    # (preserve pre-2003 records not managed by SFC)
    db_execute(
        "DELETE FROM directorships WHERE position_id IN (394,395) AND director=? "
        "AND (appt_date IS NULL OR appt_date >= '2003-04-01')",
        (person_id,),
    )

    # Insert new records
    for org_id, position_id, start, end in final:
        if start is None:
            # Check for existing pre-system record
            existing = db_fetchone(
                "SELECT id, res_date FROM directorships "
                "WHERE company=? AND director=? AND position_id=?",
                (org_id, person_id, position_id),
            )
            if existing:
                ex_end = existing["res_date"]
                if ex_end is None or ex_end > "2003-04-01":
                    db_execute(
                        "UPDATE directorships SET res_date=? WHERE id=?",
                        (end, existing["id"]),
                    )
                    continue
        db_execute(
            "INSERT INTO directorships (company, director, position_id, appt_date, res_date) "
            "VALUES (?,?,?,?,?)",
            (org_id, person_id, position_id, start, end),
        )

    db_commit()

# ---------------------------------------------------------------------------
# Individual history: fetch, update records, recompute directorships
# ---------------------------------------------------------------------------
def sfc_ind_hist(sfc_id, recomp=False):
    """Get the history of an individual and update the DB.

    If recomp=True, update directorships even if no change to licrec.
    """
    row = db_fetchone("SELECT person_id, name1, name2 FROM people WHERE sfc_id=?", (sfc_id,))
    if not row:
        return
    person_id = row["person_id"]

    records, found = read_ind_lic_records(sfc_id)
    if not found:
        return

    changed, records = update_ind_lic_rec(person_id, records)
    if not changed and not recomp:
        return

    compute_directorships(person_id, records)

# ---------------------------------------------------------------------------
# Org history: fetch and update records
# ---------------------------------------------------------------------------
def sfc_org_hist(org_person_id):
    """Update the SFC licence history of one org."""
    row = db_fetchone(
        "SELECT sfc_id, sfc_ri, name1 FROM organisations "
        "WHERE sfc_id IS NOT NULL AND person_id=?",
        (org_person_id,),
    )
    if not row:
        return
    sfc_id = row["sfc_id"]
    sfc_ri = bool(row["sfc_ri"])

    records, found, sfc_ri_new = read_org_lic_records(sfc_id, sfc_ri)
    if not found:
        return

    update_org_lic_rec(org_person_id, records, sfc_ri_new, sfc_ri)

# ---------------------------------------------------------------------------
# Staff fetching: ROs and Reps for an org
# ---------------------------------------------------------------------------
def sfc_both_ranks(sfc_id, hours):
    """Fetch both ROs and Reps for an org, then update individual histories."""
    _sfc_people_rank(sfc_id, 1, hours)
    _sfc_people_rank(sfc_id, 0, hours)

def _sfc_people_rank(sfc_id, sfc_rank, hours):
    """Get names of licensed staff of an org and put them in the DB.

    sfc_rank: 1=RO, 0=Rep
    """
    row = db_fetchone("SELECT person_id FROM organisations WHERE sfc_id=?", (sfc_id,))
    if not row:
        return

    page = "ro" if sfc_rank == 1 else "rep"
    html = get_sfc_page(page, sfc_id, "corp")
    if not html:
        return

    people = read_sfc_people(html, page)
    if not people:
        return

    for person in people:
        p_sfc_id = person["sfc_id"]
        n1 = person["name1"]
        n2 = person["name2"]
        c_name = person["c_name"]

        sfc_upd = "1970-01-01"

        # Check if person exists by SFC ID
        prow = db_fetchone(
            "SELECT person_id, sfc_upd, c_name FROM people WHERE sfc_id=?",
            (p_sfc_id,),
        )
        if prow:
            pid = prow["person_id"]
            if prow["sfc_upd"]:
                sfc_upd = prow["sfc_upd"]
            if c_name and not prow["c_name"]:
                db_execute("UPDATE people SET c_name=? WHERE person_id=?", (c_name, pid))
                db_commit()
        else:
            get_or_create_person(p_sfc_id, n1, n2, c_name)

        # Update history if not done recently
        if hours_since(sfc_upd) >= hours:
            try:
                sfc_ind_hist(p_sfc_id, recomp=False)
            except Exception as e:
                log(f"  ERROR updating history for {p_sfc_id}: {e}")

# ---------------------------------------------------------------------------
# Parallel processing helper
# ---------------------------------------------------------------------------
_progress_lock = threading.Lock()

def _run_parallel(items, worker_fn, desc="items"):
    """Process items in parallel using the thread pool.

    worker_fn(item) is called for each item. Exceptions are caught and logged.
    Returns (done_count, error_count).
    """
    total = len(items)
    if total == 0:
        return 0, 0
    done = [0]
    errors = [0]

    def wrapped(item):
        try:
            worker_fn(item)
        except Exception as e:
            with _progress_lock:
                errors[0] += 1
            log(f"  ERROR processing {desc}: {e}")
            db_commit()  # ensure partial work is saved
            return
        with _progress_lock:
            done[0] += 1
            d = done[0]
        if d % 200 == 0 or d == total:
            log(f"  [{d}/{total}] {desc} processed...")

    with ThreadPoolExecutor(max_workers=WORKER_THREADS) as executor:
        futures = [executor.submit(wrapped, item) for item in items]
        for f in as_completed(futures):
            f.result()  # propagate unexpected errors

    return done[0], errors[0]

# ---------------------------------------------------------------------------
# Step 1: Discover all corporations
# ---------------------------------------------------------------------------
def step1_discover_corps():
    """Search for all SFC-licensed corporations (active + ceased).

    Uses licstatus=all to capture full historical dataset.
    """
    log("=== Step 1: Discovering all SFC-licensed corporations ===")
    new_count = 0
    for ra_type in ACTIVITY_TYPES:
        log(f"  Activity type {ra_type}")
        for ltr in LETTERS_CORP:
            post_data = (
                f"licstatus=all&ratype={ra_type}&roleType=corporation"
                f"&nameStartLetter={ltr}&page=1&start=0&limit=9999"
            )
            resp = fetch_post(SEARCH_URL, post_data)
            items = extract_search_items(resp)
            for item in items:
                sfc_id = item.get("ceref", "")
                if not sfc_id:
                    continue
                if db_exists("SELECT 1 FROM organisations WHERE sfc_id=?", (sfc_id,)):
                    continue
                if db_exists("SELECT 1 FROM old_sfc_ids WHERE sfc_id=?", (sfc_id,)):
                    continue

                name = item.get("name", "")
                c_name = item.get("nameChi", "")
                is_corp = item.get("isCorp", True)
                ptype = "corp" if is_corp else "ri"

                try:
                    log(f"    New: {sfc_id} {name}")
                    org_id = sfc_id_to_org_id(sfc_id, name, c_name, ptype)
                    new_count += 1

                    # Get licence history
                    sfc_org_hist(org_id)

                    if is_corp:
                        # Get people (ROs and Reps)
                        sfc_both_ranks(sfc_id, 0)

                    db_execute(
                        "UPDATE organisations SET sfc_upd=? WHERE person_id=?",
                        (now_iso(), org_id),
                    )
                    db_commit()
                except Exception as e:
                    log(f"    ERROR processing corp {sfc_id}: {e}")
                    db_commit()

    log(f"  Step 1 complete: {new_count} new corporations")

# ---------------------------------------------------------------------------
# Step 2: Update staff for all known orgs
# ---------------------------------------------------------------------------
def step2_update_org_staff(force=False):
    """Update current staff of all known non-RI orgs.

    Uses thread pool for parallel HTTP fetching.
    """
    log("=== Step 2: Updating current staff of known orgs ===")
    rows = db_fetchall(
        "SELECT person_id, sfc_id, name1, sfc_upd FROM organisations "
        "WHERE sfc_id IS NOT NULL AND sfc_ri=0 ORDER BY name1"
    )
    to_update = []
    for row in rows:
        if not force:
            sfc_upd = row["sfc_upd"]
            if sfc_upd and hours_since(sfc_upd) < HOURS_THRESHOLD:
                continue
        to_update.append(row)

    hours = HOURS_THRESHOLD if not force else 0

    def process_org(row):
        sfc_id = row["sfc_id"]
        person_id = row["person_id"]
        sfc_both_ranks(sfc_id, hours)
        db_execute(
            "UPDATE organisations SET sfc_upd=? WHERE person_id=?",
            (now_iso(), person_id),
        )
        db_commit()

    log(f"  {len(to_update)} orgs to update ({WORKER_THREADS} threads)...")
    done, errs = _run_parallel(to_update, process_org, desc="org staff")
    log(f"  Step 2 complete: {done} updated, {errs} errors")

# ---------------------------------------------------------------------------
# Step 3: Update individual licence histories
# ---------------------------------------------------------------------------
def step3_update_ppl_hist(force=False):
    """Update histories of ALL people with an SFC ID.

    Uses thread pool for parallel HTTP fetching.
    """
    log("=== Step 3: Updating individual licence histories ===")
    rows = db_fetchall(
        "SELECT person_id, sfc_id, name1, name2, sfc_upd "
        "FROM people WHERE sfc_id IS NOT NULL ORDER BY name1, name2"
    )
    to_update = []
    for row in rows:
        if not force:
            sfc_upd = row["sfc_upd"]
            if sfc_upd and hours_since(sfc_upd) < HOURS_THRESHOLD:
                continue
        to_update.append(row)

    def process_person(row):
        sfc_ind_hist(row["sfc_id"], recomp=False)

    log(f"  {len(to_update)} people to update ({WORKER_THREADS} threads)...")
    done, errs = _run_parallel(to_update, process_person, desc="people hist")
    log(f"  Step 3 complete: {done} updated, {errs} errors")

# ---------------------------------------------------------------------------
# Targeted: fetch history for people missing sfc_upd
# ---------------------------------------------------------------------------
def fetch_missing_people():
    """Fetch licence histories for people who exist in DB but have no sfc_upd.

    These are people discovered (e.g. via step 5 or org staff pages) but whose
    individual history was never successfully fetched — typically due to a
    crashed or interrupted run.
    Uses thread pool for parallel HTTP fetching.
    """
    log("=== Fetching history for people with missing sfc_upd ===")
    rows = db_fetchall(
        "SELECT person_id, sfc_id, name1, name2 "
        "FROM people WHERE sfc_id IS NOT NULL AND sfc_upd IS NULL "
        "ORDER BY name1, name2"
    )
    if not rows:
        log("  No people with missing history found.")
        return
    log(f"  Found {len(rows)} people with missing history ({WORKER_THREADS} threads)...")

    def process_person(row):
        sfc_id = row["sfc_id"]
        sfc_ind_hist(sfc_id, recomp=True)
        # Mark as checked even if SFC returned no data, so we don't retry
        if not db_exists(
            "SELECT 1 FROM people WHERE sfc_id=? AND sfc_upd IS NOT NULL",
            (sfc_id,),
        ):
            db_execute(
                "UPDATE people SET sfc_upd=? WHERE sfc_id=?",
                (now_iso(), sfc_id),
            )
            db_commit()

    done, errs = _run_parallel(rows, process_person, desc="missing people")
    log(f"  Done: {done} updated, {errs} errors out of {len(rows)}")

# ---------------------------------------------------------------------------
# Step 4: Update org licence histories
# ---------------------------------------------------------------------------
def step4_update_org_hist(live_only=False):
    """Update the SFC licence history of orgs.

    live_only=True: only non-dissolved orgs (incremental mode).
    live_only=False: all orgs including dissolved (full mode).
    Uses thread pool for parallel HTTP fetching.
    """
    log("=== Step 4: Updating org licence histories ===")
    sql = "SELECT person_id, name1 FROM organisations WHERE sfc_id IS NOT NULL"
    if live_only:
        sql += " AND dis_date IS NULL"
    rows = db_fetchall(sql)

    def process_org(row):
        sfc_org_hist(row["person_id"])

    log(f"  {len(rows)} orgs to process ({WORKER_THREADS} threads)...")
    done, errs = _run_parallel(rows, process_org, desc="org hist")
    log(f"  Step 4 complete: {done} processed, {errs} errors")

# ---------------------------------------------------------------------------
# Step 5: Discover all individuals
# ---------------------------------------------------------------------------
def step5_discover_individuals():
    """Search for all SFC-licensed individuals.

    Uses licstatus=all to capture full historical dataset.
    """
    log("=== Step 5: Discovering all SFC-licensed individuals ===")
    new_count = 0
    for ra_type in ACTIVITY_TYPES:
        log(f"  Activity type {ra_type}")
        for ltr in LETTERS_INDI:
            post_data = (
                f"licstatus=all&ratype={ra_type}&roleType=individual"
                f"&nameStartLetter={ltr}&page=1&start=0&limit=99999"
            )
            resp = fetch_post(SEARCH_URL, post_data)
            items = extract_search_items(resp)
            for item in items:
                sfc_id = item.get("ceref", "")
                is_indi = item.get("isIndi", False)
                if not sfc_id or not is_indi:
                    continue
                if db_exists("SELECT 1 FROM people WHERE sfc_id=?", (sfc_id,)):
                    continue

                name = strip_space(item.get("name", ""))
                name = name.replace("\\u0027", "'")
                n1, n2 = name_split(name)
                c_name = item.get("nameChi", "")

                try:
                    log(f"    New: {sfc_id} {n1}, {n2}")
                    get_or_create_person(sfc_id, n1, n2, c_name)
                    new_count += 1
                    sfc_ind_hist(sfc_id, recomp=True)
                except Exception as e:
                    log(f"    ERROR processing individual {sfc_id}: {e}")
                    db_commit()

    log(f"  Step 5 complete: {new_count} new individuals")

# ---------------------------------------------------------------------------
# Step 6: Update addresses
# ---------------------------------------------------------------------------
def step6_update_addresses():
    """Update org addresses and websites.

    Uses thread pool for parallel HTTP fetching.
    """
    log("=== Step 6: Updating org addresses ===")
    rows = db_fetchall(
        "SELECT person_id, sfc_id, sfc_ri, name1 FROM organisations WHERE sfc_id IS NOT NULL"
    )

    def process_addr(row):
        pid = row["person_id"]
        sfc_id = row["sfc_id"]
        sfc_ri = bool(row["sfc_ri"])
        ptype = "ri" if sfc_ri else "corp"

        html = get_sfc_page("addresses", sfc_id, ptype)

        # Handle "No record found" by trying the other type
        if not html or "No record found" in html:
            alt_type = "corp" if sfc_ri else "ri"
            html = get_sfc_page("addresses", sfc_id, alt_type)
            if html and "No record found" not in html:
                sfc_ri = not sfc_ri
                ptype = alt_type
            else:
                return

        if "System error found" in html:
            return

        # Update sfc_ri if changed
        if sfc_ri != bool(row["sfc_ri"]):
            db_execute(
                "UPDATE organisations SET sfc_ri=? WHERE person_id=?",
                (1 if sfc_ri else 0, pid),
            )

        # Extract and update website
        _update_website(html, pid)

        # Extract and update address
        _update_address(html, sfc_id, ptype, pid)

        db_commit()

    log(f"  {len(rows)} orgs to process ({WORKER_THREADS} threads)...")
    done, errs = _run_parallel(rows, process_addr, desc="addresses")
    log(f"  Step 6 complete: {done} processed, {errs} errors")

def _update_website(html, pid):
    """Extract and update website from address page."""
    ws_data = extract_json_var(html, "websiteData")
    if not ws_data or not isinstance(ws_data, list) or len(ws_data) == 0:
        return
    ws = ws_data[0].get("website", "") if isinstance(ws_data[0], dict) else ""
    if not ws:
        return

    # Clean up
    for sep in ("\\u0026", ";", " /"):
        if sep in ws:
            ws = ws[:ws.index(sep)]
    ws = ws.replace("http://", "").replace("https://", "")
    ws = ws.replace("No website", "").strip()
    if not ws:
        return

    # Check if website already exists
    ws_clean = ws.replace("www.", "")
    existing = db_fetchone(
        "SELECT id, source FROM web WHERE person_id=? AND (url LIKE ? OR url=?)",
        (pid, f"%{ws_clean}%", ws),
    )
    if not existing:
        db_execute("INSERT INTO web (person_id, url, source) VALUES (?,?,2)", (pid, ws))
    elif existing["source"] != 2:
        db_execute("UPDATE web SET source=2 WHERE id=?", (existing["id"],))

def _update_address(html, sfc_id, ptype, pid):
    """Extract and update address from address page."""
    # Check if multiple addresses — if so, use complaints officer
    addr_str = ""
    if html.count('"fullAddress"') > 1:
        co_html = get_sfc_page("co", sfc_id, ptype)
        co_data = extract_json_var(co_html, "cofficerData") if co_html else None
        if co_data and isinstance(co_data, list) and len(co_data) > 0:
            item = co_data[0]
            if isinstance(item, dict):
                addr_obj = item.get("address", {})
                if isinstance(addr_obj, dict):
                    addr_str = addr_obj.get("fullAddress", "")
                else:
                    addr_str = item.get("fullAddress", "")
    else:
        addr_data = extract_json_var(html, "addressData")
        if addr_data and isinstance(addr_data, list) and len(addr_data) > 0:
            if isinstance(addr_data[0], dict):
                addr_str = addr_data[0].get("fullAddress", "")

    if not addr_str:
        return

    # Clean up address
    while ",," in addr_str:
        addr_str = addr_str.replace(",,", ",")
    addr_str = strip_space(addr_str)
    addr_str = addr_str.replace("/F.", "/F")
    addr_str = addr_str.replace("/F ", "/F, ")
    addr_str = addr_str.replace("\\u0027", "'")
    addr_str = addr_str.replace("\\u0026", "&")
    addr_str = addr_str.replace("Hong Kong, Hong Kong", "Hong Kong")

    # Add "Hong Kong" suffix for known HK districts
    hk_districts = [
        "Central", "Tsim Sha Tsui", "Chai Wan", "Sheung Wan",
        "Wanchai", "Admiralty", "Causeway Bay", "Quarry Bay",
        "North Point", "Tai Po",
    ]
    for dist in hk_districts:
        if addr_str.endswith(dist):
            addr_str += ", Hong Kong"
            break
    if addr_str.endswith(", HK"):
        addr_str = addr_str[:-2] + "Hong Kong"

    parts = [p.strip() for p in addr_str.split(",")]
    if not parts:
        return

    a1 = a2 = a3 = district = ""

    if parts[-1] == "Hong Kong":
        # HK address
        if len(parts) > 1:
            district = parts[-2]

        # Address lines: everything before district and "Hong Kong"
        addr_parts = parts[:-1]  # exclude "Hong Kong"
        if len(addr_parts) > 1:
            addr_parts = addr_parts[:-1]  # exclude district
        else:
            addr_parts = []

        # Recombine if too many parts
        while len(addr_parts) > 3:
            addr_parts[0] = addr_parts[0] + ", " + addr_parts[1]
            addr_parts.pop(1)

        if len(addr_parts) > 0:
            a1 = addr_parts[0]
        if len(addr_parts) > 1:
            a2 = addr_parts[1]
        if len(addr_parts) > 2:
            a3 = addr_parts[2]

        # Determine if we should overwrite (address is visible, not hidden)
        overwrite = "loadData({})" not in html

        existing = db_fetchone("SELECT 1 FROM orgdata WHERE person_id=?", (pid,))
        if not existing:
            db_execute(
                "INSERT INTO orgdata (person_id, addr1, addr2, addr3, district, territory) "
                "VALUES (?,?,?,?,?,1)",
                (pid, a1, a2, a3, district),
            )
        elif overwrite:
            db_execute(
                "UPDATE orgdata SET addr1=?, addr2=?, addr3=?, district=?, territory=1 "
                "WHERE person_id=?",
                (a1, a2, a3, district, pid),
            )

# ---------------------------------------------------------------------------
# Step 7: Compute monthly totals
# ---------------------------------------------------------------------------
def step7_compute_totals(upd=True):
    """Compute monthly summary totals by activity type.

    upd=True: only update latest month (incremental).
    upd=False: full recompute from scratch.
    """
    log("=== Step 7: Computing monthly summary totals ===")
    row = db_fetchone("SELECT MAX(id) as max_id FROM activity")
    max_a = row["max_id"] if row and row["max_id"] else 13

    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    for a in range(0, max_a + 1):
        start_date_str = _get_licrecsum_start(a, upd)
        if start_date_str is None:
            continue

        try:
            start_dt = datetime.strptime(start_date_str[:10], "%Y-%m-%d")
        except (ValueError, TypeError):
            continue

        end_dt = datetime.strptime(yesterday, "%Y-%m-%d")
        log(f"  Activity {a}: from {start_date_str} to {yesterday}")

        y = start_dt.year
        m = start_dt.month
        while y < end_dt.year or (y == end_dt.year and m <= end_dt.month):
            d = last_day_of_month(y, m)
            act_filter = " AND act_type=?" if a > 0 else ""
            params = [d, d]
            if a > 0:
                params.append(a)

            sql = (
                "SELECT COUNT(DISTINCT staff_id) as total, "
                "COALESCE(SUM(CASE WHEN role=1 THEN 1 ELSE 0 END), 0) as ro "
                "FROM (SELECT DISTINCT staff_id, role FROM licrec "
                "WHERE (end_date IS NULL OR end_date > ?) "
                "AND (start_date IS NULL OR start_date <= ?)"
                f"{act_filter})"
            )
            result = db_fetchone(sql, tuple(params))
            total = result["total"] if result else 0
            ro = result["ro"] if result else 0

            db_execute(
                "INSERT OR REPLACE INTO licrecsum (act_type, d, total, ro) VALUES (?,?,?,?)",
                (a, d, total, ro),
            )

            m += 1
            if m > 12:
                m = 1
                y += 1

        db_commit()
        log(f"  Activity {a}: done")

def _get_licrecsum_start(act_type, upd):
    """Determine the start date for licrecsum computation."""
    if upd:
        # Only update from the latest month we already have
        row = db_fetchone(
            "SELECT MAX(d) as max_d FROM licrecsum WHERE act_type=?", (act_type,)
        )
        if row and row["max_d"]:
            return row["max_d"]

    # Full recompute: find the earliest date
    if act_type == 0:
        has_null = db_exists("SELECT 1 FROM licrec WHERE start_date IS NULL")
        if has_null:
            return "2003-03-31"
        row = db_fetchone("SELECT MIN(start_date) as min_d FROM licrec")
        return row["min_d"] if row and row["min_d"] else None
    else:
        rec_exists = db_exists("SELECT 1 FROM licrec WHERE act_type=?", (act_type,))
        if not rec_exists:
            return None
        has_null = db_exists(
            "SELECT 1 FROM licrec WHERE start_date IS NULL AND act_type=?", (act_type,)
        )
        if has_null:
            return "2003-03-31"
        row = db_fetchone(
            "SELECT MIN(start_date) as min_d FROM licrec WHERE act_type=?", (act_type,)
        )
        return row["min_d"] if row and row["min_d"] else None

# ---------------------------------------------------------------------------
# Main update flow
# ---------------------------------------------------------------------------
def sfc_update(full=False):
    """Main update entry point."""
    log(f"SFC update started at {now_iso()} (full={full})")
    db_execute(
        "INSERT INTO update_log (event, timestamp) VALUES (?,?)",
        ("SFCstart", now_iso()),
    )
    db_commit()

    try:
        # Step 1: Discover all corporations
        step1_discover_corps()

        # Step 2: Update current staff of known orgs
        step2_update_org_staff(force=full)

        # Step 3: Update individual licence histories (all people with SFC ID)
        step3_update_ppl_hist(force=full)

        # Step 4: Update org licence histories
        step4_update_org_hist(live_only=not full)

        # Step 5: Discover all individuals
        step5_discover_individuals()

        # Step 6: Update addresses
        step6_update_addresses()

        # Step 7: Compute monthly totals (full recompute due to SFC late-edits)
        step7_compute_totals(upd=False)

    except KeyboardInterrupt:
        log("Interrupted by user. Progress has been saved.")
    except Exception as e:
        log(f"SFC update FAILED: {e}")
        import traceback
        traceback.print_exc(file=sys.stderr)
    finally:
        db_execute(
            "INSERT INTO update_log (event, timestamp) VALUES (?,?)",
            ("SFCend", now_iso()),
        )
        db_commit()
        log(f"SFC update ended at {now_iso()}")

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Update SFC licensee SQLite database from the SFC public register."
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Force complete re-fetch ignoring sfc_upd timestamps.",
    )
    parser.add_argument(
        "--missing",
        action="store_true",
        help="Only fetch history for people already in DB but missing sfc_upd.",
    )
    parser.add_argument(
        "--step5",
        action="store_true",
        help="Only run Step 5: discover all individuals (including ceased).",
    )
    parser.add_argument(
        "--step6",
        action="store_true",
        help="Only run Step 6: update org addresses and websites.",
    )
    parser.add_argument(
        "--step7",
        action="store_true",
        help="Only run Step 7: compute monthly licence totals.",
    )
    parser.add_argument(
        "--db",
        metavar="PATH",
        default=None,
        help=(
            "Path to the SQLite database file. "
            "Defaults to ./sfc.db (override with $SFC_DB_PATH)."
        ),
    )
    args = parser.parse_args()

    if args.db:
        global DB_PATH
        DB_PATH = os.path.abspath(args.db)

    if args.step5 or args.step6 or args.step7:
        get_db()  # ensure DB is initialized
        if args.step5:
            log(f"SFC Step 5 (discover individuals) started at {now_iso()}")
            step5_discover_individuals()
            log(f"SFC Step 5 ended at {now_iso()}")
        if args.step6:
            log(f"SFC Step 6 (update addresses) started at {now_iso()}")
            step6_update_addresses()
            log(f"SFC Step 6 ended at {now_iso()}")
        if args.step7:
            log(f"SFC Step 7 (compute totals) started at {now_iso()}")
            step7_compute_totals(upd=False)
            log(f"SFC Step 7 ended at {now_iso()}")
    elif args.missing:
        log(f"SFC missing-history fetch started at {now_iso()}")
        get_db()  # ensure DB is initialized
        fetch_missing_people()
        log(f"SFC missing-history fetch ended at {now_iso()}")
    else:
        sfc_update(full=args.full)

if __name__ == "__main__":
    main()
