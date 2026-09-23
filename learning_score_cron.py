#!/usr/bin/env python3
"""
Learning Score — Monthly, User-wise Cron
==========================================
Pulls per-user data from Metabase, scores it, and writes one tab per month
into a Google Sheet — built to slot alongside your existing GitHub Actions
crons (Module_contest pipeline / Data Pipeline Automation), reusing the same
auth, retry, and Sheets-writing patterns so it can live in the same repo.

FIXED SINCE THE FIRST RUN (2026-09-15 GitHub Actions failures):
  - Card #8646 turned out to be lecture-level, not user-level (confirmed from
    the actual error: columns were lecture_id/batch_strength/overall_viewers/
    etc, no user_id) — swapped attendance to card #11636
    (user_level_lecture_level_attendance_time_spent), which IS genuinely
    user+lecture-level (verified via its saved query) and needed no merge
    with #6031 at all. #6031 is no longer fetched.
  - Card #7939 was flagged as a risk (used batch-wise in your existing
    pipeline) but its saved query confirms it IS user-level
    (user_id/users_completion_rate/users_completion_rate_on_time). It also
    has a `Date` template-tag filter on assignment release date — this
    script now passes that parameter explicitly so each month's tab reflects
    assignments released that month, not an all-time cumulative number.
  - Card #11636's actual output has 'overall_attendance' (0/100, already
    scaled), not 'overall_attended_flag' as first assumed — confirmed from
    the second run's real column list. Fixed to use overall_attendance.
  - Added student_name / email / phone to the output. No new card needed —
    the roster card (#6289) already selects
    concat(first_name,' ',last_name) as student_name, auth_user.email, and
    users_userprofile.phone in its own saved SQL (verified via
    metabase://question/6289 + metabase://table/211/fields — phone lives on
    the "Newton School" DB's users_userprofile table, NOT the Data-Science-DB
    one, which has no phone column at all). build_roster() now keeps those
    columns and they flow straight into the final merged sheet.
  - Added, then fully REMOVED, a Placement Correlation step (read the
    "Placements - FlyWheel" sheet, write a "Placement Corr" tab here). You're
    now maintaining "Placement Corr" yourself as a manual copy of the
    Prog<>Placement data, and doing the correlation analysis separately —
    so per your instruction this cron no longer touches the Placements sheet
    at all, in either direction. If you want this automated again later, the
    prior approach (join on user_id, correlate avg learning_score against
    is_placed) is straightforward to re-add — just say so.
  - REMOVED build_placement_tags() entirely (it read the "Groomers and
    Master Data 2026" sheet for reference-only "Placement Profile Tags"
    columns) — per your request this script now ignores every placement-
    related sheet, not just "Placements - FlyWheel". GROOMERS_SHEET_KEY /
    GROOMERS_SHEET_TAB config, the ENV CHECK line for it, and the merge into
    the output df are all gone too. Nothing in this script reads or writes
    any Google Sheet except the Learning Score sheet itself.

STILL OPEN — fix these before trusting the numbers:

  1. Arena / Playlist section has NO confirmed source card. Your two existing
     pipelines never pull it. `build_arena()` is a stub that raises
     NotImplementedError until you give me a card ID (or confirm the
     `arena_questions_user_mapping` table + its difficulty join).
  2. TA Sessions uses card #9251 as a best guess — it does not appear in
     either of your existing pipelines (which have mentor-ops cards 7019 /
     6161 / 6184 / 7941 / 6167 instead, all mentor-centric rather than
     obviously "sessions a student attended"). Confirm or swap.
  3. Card #11636 (attendance) explicitly EXCLUDES batches with "advantage" or
     "agentic" in the name (`au_batch_name NOT ILIKE '%advantage%'` /
     `'%agentic%'`) — if your Learning Score cohort includes those tracks,
     their attendance will come back null. Flag if so and I'll find/build an
     unfiltered version.

FIXED SINCE THE 2026-09-21 FAILURE — the real traceback showed the actual
cause was card #7577 (grooming sessions) hitting a transient Postgres error
after 154s: "canceling statement due to conflict with recovery / User was
holding shared buffer pin for too long" — a hot-standby read-replica
cancelling a slow query mid-WAL-replay, NOT the huge 11636/9251 payloads I'd
initially (and wrongly) suspected from the truncated log. metabase_request()
now retries that specific error signature (and a couple of close cousins —
statement timeout, deadlock detected) with the same backoff it already used
for connection errors, instead of failing the whole run on the first hit —
see _is_transient_query_error(). The two payload-size fixes below are still
worth having (9251 was genuinely being pulled all-time for no reason) but
weren't what broke this particular run:
  - Card #9251 (TA sessions) actually HAS a `{{date}}` template tag on its
    saved query (bound to video_sessions_onetoone.start_timestamp, tag id
    e265d2ea-9732-4bf3-ab63-fa42ea82b7f8) that this script simply wasn't
    using — it was in the blind unscoped prefetch, so every run pulled the
    ENTIRE 1:1-session history. Moved it out of prefetch_all_cards() and into
    build_sessions(), fetched with that date parameter set to the target
    month, the same way build_assignments() already scopes card #7939. This
    should shrink its payload from ~25MB all-time to a few hundred KB/month.
  - Card #11636 (attendance) has NO date/range template tag on its saved
    query at all (checked its saved SQL directly) — the `start_timestamp >
    '01-01-2025'` bound is hardcoded in the query text, not parameterized, so
    there's no equivalent server-side scoping fix available from this script.
    It will keep growing every month as more lectures happen since Jan 2025.
    If the 160MB payload turns out to be what's actually killing the run
    (once you get me the real traceback — OOM on the runner, a timeout, etc.)
    the next options are: (a) ask whoever owns that saved question in
    Metabase to add a date template tag to it, mirroring #9251/#7939, or (b)
    switch this script to fetch it via /query/csv instead of /query/json —
    CSV is meaningfully more compact than JSON for a ~30-column table since
    JSON repeats every column name on every row. Not done yet since I
    haven't confirmed memory/timeout is actually what's failing.

ADDED 2026-09-22 — BACKFILL MODE, to build up enough historical months to
correlate learning_score against placement outcomes:
  - Set RUN_MONTH_MODE=backfill (plus BACKFILL_START_MONTH, e.g. "2025-12",
    and optionally BACKFILL_END_MONTH — defaults to the last fully completed
    month) and this script now scores and writes EVERY month in that range in
    one run, not just one. target_month() became target_months(), returning
    a list — one entry for "previous"/"current" mode (unchanged behavior),
    one entry per month for "backfill".
  - The expensive all-time card fetches (roster, attendance #11636, module
    contests, projects, grooming sessions) still happen exactly ONCE via
    prefetch_all_cards() and build_roster() before the month loop — each is
    all-time data already filtered per-month client-side (in_target_month()),
    so backfilling 8 more months costs nothing extra there. Only the two
    cards fetched with a server-side date parameter (assignments #7939, TA
    sessions #9251) are re-fetched once per backfilled month — that's
    necessary, not wasteful, since that's the whole point of scoping them.
  - One month failing (e.g. another transient DB hiccup like the 7577 one
    above) no longer kills the rest of the backfill — each month's
    build+write is now wrapped individually, failures are collected and
    reported at the end, and the run only exits non-zero if at least one
    month failed. Re-running the same range afterward is safe/idempotent —
    write_sheet() clears and rewrites each tab from scratch either way.
  - Run it locally or via workflow_dispatch (see the .yml — now has
    run_month_mode: backfill as an option plus backfill_start_month /
    backfill_end_month inputs, and a longer timeout to match).

Everything else (roster, attendance, assignments, module contests, projects,
grooming-session count) is now verified against actual Metabase query
definitions or your two production scripts' real output — see CARD IDS below.

ADDED 2026-09-23 — STUDENT MASTER VIEW + LOOKUP, so you get one combined
view instead of having to rebuild "Placements x Batch" by hand each time:
  - After every run (whatever RUN_MONTH_MODE), this script now also
    rebuilds two extra tabs in the same Learning Score sheet:
      • "Student Master View" — one row per student: every scored month's
        learning_score (columns discovered automatically from whichever
        month tabs currently exist in the sheet — nothing hardcoded, so it
        stays current as you backfill/score more months), avg score, trend,
        the LATEST month's sub-scores (attendance/assignment/module
        contest/project/session/arena), and — best-effort — placement
        outcome + persona.
      • "Student Lookup" — two input cells (batch name / user ID) plus a
        live FILTER() pulling the matching row(s) out of Student Master
        View. Fill in either box (or both, or neither for everyone) and
        the matches appear underneath. This is the ".. just fill the batch
        name and user id .." view you asked for.
  - IMPORTANT re: the placements-sheet boundary from earlier — this reads
    the "Placement Corr" tab, which lives INSIDE this same Learning Score
    sheet (the one you already maintain yourself via IMPORTRANGE). The
    external "Placements - FlyWheel" sheet is still never touched, in
    either direction — that boundary from your "ignore the placements
    sheet" instruction hasn't changed. If "Placement Corr" is missing,
    renamed, or its columns don't match what's expected (UserID / f /
    Placed / Placement Month / Placeability / Persona / Persona UPDATED /
    Persona (Sai sheet) — confirmed against your actual tab), this step
    just logs a warning and leaves those columns blank; it never fails the
    run, since the per-month scoring is the part that actually matters.
  - Built in Python/pandas, not spreadsheet formulas — faster to open,
    nothing to break across Excel/Sheets/LibreOffice, and it fully
    rebuilds (clear + rewrite) every run, so it's always in sync with
    whatever month tabs currently exist.
"""
import os
import re
import sys
import json
import time
import calendar
import traceback
import concurrent.futures as cf
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
import pandas as pd
import numpy as np
import gspread
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials

start_time = time.time()
IST = ZoneInfo("Asia/Kolkata")

# ═══════════════════════════════════════════════════════════════════════════
# ENV & AUTH — same two secrets your other crons already use
# ═══════════════════════════════════════════════════════════════════════════
METABASE_API_KEY = os.getenv("METABASE_API_KEY")
SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON")

# Learning Score output sheet:
# https://docs.google.com/spreadsheets/d/1AJZnBpHeE85eDYWNsj-Kz91PSyG0uo8iP8PQwRsS3vU/
# Defaulted here so the cron works with just the two existing secrets — still
# overridable via env var if you ever point this at a different sheet.
# ⚠ Share this sheet (Editor) with your service account's client_email
#   (printed by the ENV CHECK below) or every write will fail with a
#   permission error.
DEFAULT_LEARNING_SCORE_SHEET_KEY = "1AJZnBpHeE85eDYWNsj-Kz91PSyG0uo8iP8PQwRsS3vU"
LEARNING_SCORE_SHEET_KEY = os.getenv("LEARNING_SCORE_SHEET_KEY", DEFAULT_LEARNING_SCORE_SHEET_KEY)

# NOTE: this script still never reads or writes any EXTERNAL placements
# sheet — the "Placements - FlyWheel" correlation step (removed earlier) and
# the "Groomers and Master Data 2026" sheet's build_placement_tags() (also
# removed) both stay gone, per your instruction. As of 2026-09-23 it DOES
# read one tab — "Placement Corr" — but that tab lives INSIDE this same
# Learning Score sheet (you maintain it yourself via IMPORTRANGE from the
# external sheet), used only to build the "Student Master View" / "Student
# Lookup" tabs below. See the file docstring's 2026-09-23 note for the full
# explanation and why this doesn't reopen the door you asked closed.

# "previous" (default) scores last calendar month — run this on/after the
# 1st and it scores the month that just ended. "current" scores month-to-date.
# "backfill" scores EVERY month from BACKFILL_START_MONTH through
# BACKFILL_END_MONTH (inclusive) in one run — see target_months() and the
# file docstring's 2026-09-22 note.
RUN_MONTH_MODE = os.getenv("RUN_MONTH_MODE", "previous")

# Backfill mode only. Both accept "YYYY-MM" (e.g. "2025-12") or "Mon-YYYY"
# (e.g. "Dec-2025"). BACKFILL_START_MONTH is required when RUN_MONTH_MODE=
# backfill; BACKFILL_END_MONTH is optional and defaults to the last fully
# completed calendar month (same month "previous" mode would score).
BACKFILL_START_MONTH = os.getenv("BACKFILL_START_MONTH")
BACKFILL_END_MONTH = os.getenv("BACKFILL_END_MONTH")

missing = [n for n, v in [
    ("METABASE_API_KEY", METABASE_API_KEY),
    ("SERVICE_ACCOUNT_JSON", SERVICE_ACCOUNT_JSON),
] if not v]
if missing:
    raise ValueError(f"❌ Missing environment variables: {', '.join(missing)}")

service_info = json.loads(SERVICE_ACCOUNT_JSON)
creds = Credentials.from_service_account_info(
    service_info,
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ],
)
gc = gspread.authorize(creds)

METABASE_BASE = "https://metabase-lierhfgoeiwhr.newtonschool.co"
METABASE_HEADERS = {"Content-Type": "application/json", "X-Api-Key": METABASE_API_KEY}

print("🔎 ENV CHECK")
print(f"   Metabase API key   : {'[SET]' if METABASE_API_KEY else '[MISSING]'}")
print(f"   SA client_email    : {service_info.get('client_email')}  (share the Sheet with this)")
print(f"   Learning Score sheet: {LEARNING_SCORE_SHEET_KEY}")

# Transport-level retries for transient network blips, same as your existing scripts
SESSION = requests.Session()
_retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
_adapter = HTTPAdapter(max_retries=_retry, pool_maxsize=8)
SESSION.mount("https://", _adapter)
SESSION.mount("http://", _adapter)


# ═══════════════════════════════════════════════════════════════════════════
# CARD IDS — with source/confidence notes. See file docstring for the gaps.
# ═══════════════════════════════════════════════════════════════════════════
ROSTER_CARD = 6289                 # Confirmed: used by every section in both existing pipelines.
                                    # Gives user_id, label, au_batch_name, gem_label. Filtered to
                                    # label in {Enrolled, DS Advantage, Advantage +} = active students.

CONTEST_MCQ_CARD = 8057            # Confirmed: Module_contest pipeline, MCQ scores per user/module.
CONTEST_CODING_CARD = 6391         # Confirmed: Module_contest pipeline, coding scores per user/module.

ATTENDANCE_CARD = 11636            # Confirmed via saved-query inspection: genuinely user+lecture-level.
                                    # Columns include user_id, lecture_id, lecture_start_timestamp,
                                    # overall_attended_flag, time_spent_mins, au_batch_name, gem_label.
                                    # ⚠ excludes 'advantage'/'agentic' batches by name — see docstring #3.
                                    # (#6031 + #8646, used previously, are no longer fetched — #8646
                                    # turned out to be lecture-level only, no user_id.)

ASSIGNMENTS_CARD = 7939            # Confirmed via saved-query inspection: user-level. Has a `Date`
                                    # template-tag on assignment release date — queried WITH that
                                    # parameter set to the target month (see build_assignments()),
                                    # NOT part of the blind concurrent prefetch below.
ASSIGNMENTS_DATE_TAG_ID = "2ee32c9b-aa6d-455a-8998-fe61db513efc"
                                    # The "Date" template tag's own id on card #7939, pulled straight
                                    # from its saved query definition (Metabase requires this exact id
                                    # in the parameter payload, confirmed by the 400 error otherwise —
                                    # see date_range_param()). If this card's Date filter is ever
                                    # deleted and re-added (not just edited), Metabase will assign it a
                                    # new id and this constant will need updating to match.

TA_SESSIONS_DATE_TAG_ID = "e265d2ea-9732-4bf3-ab63-fa42ea82b7f8"
                                    # Card #9251's own `{{date}}` template tag id (tag name "date",
                                    # widget-type date/range, bound to
                                    # video_sessions_onetoone.start_timestamp) — confirmed straight from
                                    # its saved query. Previously unused, so every run pulled the card's
                                    # entire all-time history (~25MB) instead of just the target month;
                                    # now passed explicitly, same pattern as ASSIGNMENTS_DATE_TAG_ID
                                    # above. See the file docstring's 2026-09-21 fix note.

PROJECTS_RAW_CARDS = (6241, 6242)  # Confirmed: Data Pipeline "Projects Raw" — user-level, has
                                    # Submission Time / marks_obtained / project_deadline_date.
PROJECT_EVAL_CARDS = (6578, 6579)  # Confirmed: Data Pipeline "Project Evaluations".

GROOMING_SESSIONS_CARD = 7577      # From the DS Learning Queries collection — actively maintained
                                    # (refactor notes dated this month). Not in either existing
                                    # pipeline, but is the only real candidate found.

TA_SESSIONS_CARD = 9251            # ⚠ BEST GUESS — not used by either existing pipeline. Confirm this
                                    # is really "1:1 sessions a student attended" and not something else.
                                    # Has a confirmed `{{date}}` template tag (see TA_SESSIONS_DATE_TAG_ID)
                                    # — fetched separately, month-scoped, inside build_sessions(), same
                                    # as ASSIGNMENTS_CARD below. NOT part of the blind prefetch.

ARENA_CARD = None                  # ⚠ NOT IDENTIFIED — see build_arena() below.

# ASSIGNMENTS_CARD and TA_SESSIONS_CARD are deliberately excluded — they're
# fetched separately, each with its own month-scoped date parameter, inside
# build_assignments() / build_sessions() respectively.
REQUIRED_CARDS = [
    ROSTER_CARD, CONTEST_MCQ_CARD, CONTEST_CODING_CARD,
    ATTENDANCE_CARD,
    *PROJECTS_RAW_CARDS, *PROJECT_EVAL_CARDS,
    GROOMING_SESSIONS_CARD,
]

# Module labels as they appear in the Learning Score sheet -> module_name
# values as they appear in Metabase (confirmed via calculate_total_score()
# in your Module_contest pipeline — the order/naming lines up almost exactly).
MODULE_NAME_MAP = {
    "Excel":  "DS 02 Spreadsheets",
    "SQL":    "DS 04 SQL",
    "Python": "DS 05 Python",
    "EDA - 1": "DS 06 EDA 1",
    "EDA - 2": "DS 07 EDA 2",   # MCQ-only, per your existing calculate_total_score()
}
MCQ_ONLY_MODULES = {"DS 03 Power BI", "DS 07 EDA 2"}


# ═══════════════════════════════════════════════════════════════════════════
# METABASE FETCH — same shape as your Module_contest pipeline's
# metabase_request()/fetch_card(), trimmed slightly for one script's worth
# of cards (11, vs. that pipeline's 11 too — concurrency=3 kept for the same
# reason: avoid hammering cards that are known to hard-reset under load).
# ═══════════════════════════════════════════════════════════════════════════
_card_cache = {}


def month_date_range_value(y, m):
    """'YYYY-MM-01~YYYY-MM-DD' (last day of month) — the value format Metabase
    expects for a date/range template-tag parameter."""
    last_day = calendar.monthrange(y, m)[1]
    return f"{y:04d}-{m:02d}-01~{y:04d}-{m:02d}-{last_day:02d}"


def date_range_param(tag_name, y, m, tag_id):
    """A Metabase `parameters` entry that binds a date/range value to a
    native-query template tag named `tag_name` (e.g. the `{{Date}}` tag on
    card #7939). Pass this in fetch_card(..., parameters=[...]) for any card
    that needs to be scoped to the target month server-side rather than
    filtered client-side after the fact (necessary for cards — like 7939 —
    that return pre-aggregated numbers with no row-level date to filter on).

    `tag_id` MUST be that template tag's own "id" (a UUID) as it appears in
    the card's saved query definition — Metabase's /api/card/{id}/query/json
    endpoint rejects a parameter with no "id" ("missing required key,
    received: nil", confirmed against a real 400 response), it's not enough
    to just name the tag via `target`. This id is per-card and per-tag, not
    something this script can derive on its own — see ASSIGNMENTS_DATE_TAG_ID
    below for where card #7939's was pulled from and how to find another."""
    return {
        "id": tag_id,
        "type": "date/range",
        "target": ["dimension", ["template-tag", tag_name]],
        "value": month_date_range_value(y, m),
    }


def _is_transient_query_error(res):
    """Some queries come back as HTTP 400 where the body shows the DATABASE
    itself cancelled the query rather than Metabase rejecting a bad request —
    confirmed from a real 2026-09-21 failure on card #7577:
        "ERROR: canceling statement due to conflict with recovery
         Detail: User was holding shared buffer pin for too long."
    That's Postgres on a hot-standby/read replica cancelling a slow query
    because it held a buffer pin too long while WAL replication was catching
    up — a timing coincidence with replica load, not a bad query or a bad
    parameter. It (and its cousins below) normally succeeds a moment later,
    so these are worth retrying with backoff exactly like a connection error.
    Anything else that comes back as HTTP 400 (e.g. a malformed parameter)
    still fails fast — no signature match, no retry."""
    if res.status_code != 400:
        return False
    text = res.text.lower()
    return any(sig in text for sig in (
        "conflict with recovery",
        "shared buffer pin",
        "statement timeout",
        "deadlock detected",
    ))


def metabase_request(card_id, label=None, timeout=480, max_conn_retries=5,
                      conn_backoff=30, max_conn_backoff=240, parameters=None):
    label = label or f"card {card_id}"
    url = f"{METABASE_BASE}/api/card/{card_id}/query/json"
    body = {"parameters": parameters} if parameters else None
    backoff = conn_backoff
    for attempt in range(1, max_conn_retries + 1):
        time.sleep(2)
        suffix = f" (retry {attempt}/{max_conn_retries})" if attempt > 1 else ""
        print(f"→ Fetching {label}{suffix}{' with params' if parameters else ''}...")
        t0 = time.time()
        try:
            res = SESSION.post(url, headers=METABASE_HEADERS, json=body, timeout=timeout)
        except requests.exceptions.Timeout:
            raise RuntimeError(f"⏱️ Timed out fetching {label} after {timeout}s ({url}).")
        except requests.exceptions.ConnectionError as e:
            elapsed = time.time() - t0
            if attempt == max_conn_retries:
                raise RuntimeError(f"🔌 Connection error fetching {label} after {max_conn_retries} attempts ({url}): {e}")
            print(f"🔌 Connection error after {elapsed:.1f}s: {e} — retrying in {backoff}s...")
            time.sleep(backoff)
            backoff = min(backoff * 2, max_conn_backoff)
            continue
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"❌ Request failed fetching {label} ({url}): {e}")
        elapsed = time.time() - t0
        if res.status_code != 200:
            if _is_transient_query_error(res) and attempt < max_conn_retries:
                print(f"⚠️  {label} hit a transient DB-side query cancellation after {elapsed:.1f}s "
                      f"(replica WAL-replay conflict, not a bad query) — retrying in {backoff}s...\n"
                      f"{res.text[:300]}")
                time.sleep(backoff)
                backoff = min(backoff * 2, max_conn_backoff)
                continue
            raise RuntimeError(f"❌ {label} returned HTTP {res.status_code} after {elapsed:.1f}s.\n{res.text[:500]}")
        print(f"✓ {label} done in {elapsed:.1f}s — {len(res.text)} bytes")
        return res


def fetch_card(card_id, label=None, optional=False, parameters=None):
    label = label or f"card {card_id}"
    cache_key = (card_id, json.dumps(parameters, sort_keys=True) if parameters else "")
    if cache_key in _card_cache:
        print(f"↺ Reusing cached {label}")
        return _card_cache[cache_key]
    try:
        res = metabase_request(card_id, label, parameters=parameters)
        data = res.json()
    except (RuntimeError, requests.exceptions.JSONDecodeError) as e:
        if optional:
            print(f"⚠️  {label} failed/empty and is being SKIPPED: {e}")
            return None
        raise
    _card_cache[cache_key] = data
    return data


def fetch_card_df(card_id, label=None, optional=False, require_user_id=False, parameters=None):
    """fetch_card(), as a DataFrame, with an optional loud check that the
    card is actually user-level — defense in depth in case a card gets
    swapped later for one that isn't (see file docstring for what's already
    been verified vs. still open)."""
    data = fetch_card(card_id, label, optional=optional, parameters=parameters)
    if data is None:
        return None
    df = pd.DataFrame(data)
    if require_user_id:
        user_col = next((c for c in df.columns if c.lower() in ("user_id", "user id")), None)
        if user_col is None:
            raise RuntimeError(
                f"❌ Card {card_id} ({label}) has no user_id column — columns are "
                f"{list(df.columns)}. This card looks batch/module-level, not user-level. "
                f"See file docstring item 3/4 — swap in a per-user card here instead."
            )
        if user_col != "user_id":
            df = df.rename(columns={user_col: "user_id"})
    return df


def prefetch_all_cards():
    print("\n" + "=" * 60)
    print(f"PREFETCHING {len(REQUIRED_CARDS)} CARDS")
    print("=" * 60)
    t0 = time.time()
    with cf.ThreadPoolExecutor(max_workers=3) as pool:
        futs = {pool.submit(fetch_card, cid): cid for cid in REQUIRED_CARDS}
        errors = []
        for fut in cf.as_completed(futs):
            cid = futs[fut]
            try:
                fut.result()
            except Exception as e:
                errors.append(f"card {cid}: {e}")
        if errors:
            raise RuntimeError("❌ Required card fetch(es) failed:\n  " + "\n  ".join(errors))
    print(f"✅ Prefetch done in {time.time() - t0:.0f}s — {len(_card_cache)} cards cached")


# ═══════════════════════════════════════════════════════════════════════════
# MONTH WINDOW
# ═══════════════════════════════════════════════════════════════════════════
def _parse_year_month(s, label):
    """Parses 'YYYY-MM' (e.g. '2025-12') or 'Mon-YYYY' (e.g. 'Dec-2025') into
    (year, month). Used only by backfill mode's BACKFILL_START_MONTH /
    BACKFILL_END_MONTH env vars."""
    s = s.strip()
    if "-" in s:
        left, right = s.split("-", 1)
        if left.isdigit() and len(left) == 4:
            return int(left), int(right)
        month_abbrs = {abbr.lower(): i for i, abbr in enumerate(calendar.month_abbr) if abbr}
        m = month_abbrs.get(left.strip().lower()[:3])
        if m and right.strip().isdigit():
            return int(right), m
    raise ValueError(
        f"❌ Couldn't parse {label}={s!r} — expected 'YYYY-MM' (e.g. '2025-12') "
        f"or 'Mon-YYYY' (e.g. 'Dec-2025')."
    )


def _add_months(y, m, n):
    total = (y * 12 + (m - 1)) + n
    return total // 12, total % 12 + 1


def _last_completed_month(now):
    first_of_this_month = now.replace(day=1)
    last_month_end = first_of_this_month - timedelta(days=1)
    return last_month_end.year, last_month_end.month


def target_months():
    """Returns a list of (year, month, tab_name) tuples for this run.

    "previous" / "current" (default) — a single-element list, exactly as
    target_month() used to return before backfill mode existed.

    "backfill" — one element per calendar month from BACKFILL_START_MONTH
    through BACKFILL_END_MONTH (inclusive, chronological order).
    BACKFILL_END_MONTH defaults to the last fully completed month (same
    month "previous" mode would score) if not set."""
    now = datetime.now(IST)

    if RUN_MONTH_MODE == "backfill":
        if not BACKFILL_START_MONTH:
            raise ValueError("❌ RUN_MONTH_MODE=backfill requires BACKFILL_START_MONTH to be set (e.g. '2025-12').")
        start_y, start_m = _parse_year_month(BACKFILL_START_MONTH, "BACKFILL_START_MONTH")
        if BACKFILL_END_MONTH:
            end_y, end_m = _parse_year_month(BACKFILL_END_MONTH, "BACKFILL_END_MONTH")
        else:
            end_y, end_m = _last_completed_month(now)

        months = []
        y, m = start_y, start_m
        while (y, m) <= (end_y, end_m):
            months.append((y, m, f"{calendar.month_abbr[m]}-{y}"))
            y, m = _add_months(y, m, 1)
        if not months:
            raise ValueError(
                f"❌ Backfill range is empty — BACKFILL_START_MONTH={BACKFILL_START_MONTH!r} "
                f"is after the end month ({end_y:04d}-{end_m:02d})."
            )
        return months

    if RUN_MONTH_MODE == "current":
        y, m = now.year, now.month
    else:  # "previous"
        y, m = _last_completed_month(now)
    return [(y, m, f"{calendar.month_abbr[m]}-{y}")]


def in_target_month(series, y, m):
    dt = pd.to_datetime(series, errors="coerce")
    return (dt.dt.year == y) & (dt.dt.month == m)


# ═══════════════════════════════════════════════════════════════════════════
# ROSTER
# ═══════════════════════════════════════════════════════════════════════════
def build_roster():
    df = fetch_card_df(ROSTER_CARD, "roster (6289)", require_user_id=True)
    df = df[df["label"].isin(["Enrolled", "DS Advantage", "Advantage +"])]

    # Card #6289's own saved SQL already selects everything needed for
    # contact info — no extra card/query required:
    #   concat(auth_user.first_name,' ',auth_user.last_name) as student_name
    #   auth_user.username
    #   auth_user.email                  (selected twice in the SQL — Metabase
    #                                      may come back as "email"/"email_2",
    #                                      both handled below)
    #   users_userprofile.phone
    # (verified via metabase://question/6289's saved query_json — this is the
    # same "Newton School" DB / users_userprofile.phone confirmed to exist via
    # metabase://table/211/fields; the Data-Science-DB users_userprofile table
    # at id 11394 does NOT have phone, so this roster card is the right source.)
    if "email" not in df.columns and "email_2" in df.columns:
        df = df.rename(columns={"email_2": "email"})
    elif "email_2" in df.columns:
        df = df.drop(columns=["email_2"])  # duplicate of "email", drop it

    keep = [c for c in [
        "user_id", "student_name", "username", "email", "phone",
        "au_batch_name", "label", "gem_label",
    ] if c in df.columns]
    missing_contact = {"student_name", "email", "phone"} - set(keep)
    if missing_contact:
        print(f"⚠️  Roster card {ROSTER_CARD} is missing expected contact column(s) "
              f"{missing_contact} this run — columns were: {list(df.columns)}. "
              f"Name/email/phone will be blank for everyone until this is fixed.")
    return df[keep].drop_duplicates(subset="user_id")


# ═══════════════════════════════════════════════════════════════════════════
# ATTENDANCE  → attendance_score (0-100)
# Card #11636 is genuinely user+lecture-level (verified from its saved native
# SQL) — one row per user per lecture, with an explicit attended flag and
# watch-time in minutes, so no merge with a separate lecture-calendar card is
# needed at all.
# ═══════════════════════════════════════════════════════════════════════════
def build_attendance(y, m):
    df = fetch_card_df(ATTENDANCE_CARD, "attendance (11636)", require_user_id=True)

    # Confirmed against the actual run's error output (2026-09-15): the card
    # emits 'overall_attendance' (already scaled 0/100 — overall_attended_flag
    # itself is NOT in the output, only live_attended_flag is raw), not
    # 'overall_attended_flag' as first assumed. Using the real column name now.
    required = {"lecture_id", "lecture_start_timestamp", "overall_attendance", "time_spent_mins"}
    missing_cols = required - set(df.columns)
    if missing_cols:
        raise RuntimeError(
            f"❌ Card {ATTENDANCE_CARD} is missing expected column(s) {missing_cols} — "
            f"it may have changed since this was last verified. Columns were: {list(df.columns)}"
        )

    df["lecture_start_timestamp"] = pd.to_datetime(df["lecture_start_timestamp"], errors="coerce")
    df = df[in_target_month(df["lecture_start_timestamp"], y, m)]
    if df.empty:
        print(f"⚠️  No attendance rows for {calendar.month_name[m]} {y} "
              f"(remember: card {ATTENDANCE_CARD} excludes 'advantage'/'agentic' batches — see docstring).")
        return pd.DataFrame(columns=["user_id", "no_of_attended", "sessions_in_scope", "avg_time", "attendance_score"])

    out = df.groupby("user_id").agg(
        sessions_in_scope=("lecture_id", "nunique"),
        no_of_attended=("overall_attendance", lambda s: (s == 100).sum()),
        avg_time=("time_spent_mins", "mean"),
    ).reset_index()
    out["attendance_score"] = (out["no_of_attended"] / out["sessions_in_scope"]).clip(0, 1) * 100
    return out


# ═══════════════════════════════════════════════════════════════════════════
# ASSIGNMENTS  → assignment_score (0-100)
# Card #7939 is genuinely user-level (verified from its saved native SQL) and
# already returns pre-computed 0-100 completion percentages — but as ONE
# cumulative row per user per module, no row-level completion date. Its
# `Date` template tag scopes which assignments count by RELEASE date, so
# it's fetched here with that parameter set to the target month, separately
# from the blind prefetch (which can't parameterize per-card).
# ═══════════════════════════════════════════════════════════════════════════
def build_assignments(y, m):
    df = fetch_card_df(
        ASSIGNMENTS_CARD, "assignments (7939, month-scoped)", require_user_id=True,
        parameters=[date_range_param("Date", y, m, ASSIGNMENTS_DATE_TAG_ID)],
    )

    ontime_col, overall_col = "users_completion_rate_on_time", "users_completion_rate"
    missing_cols = {ontime_col, overall_col} - set(df.columns)
    if missing_cols:
        raise RuntimeError(
            f"❌ Card {ASSIGNMENTS_CARD} is missing expected column(s) {missing_cols} — "
            f"it may have changed since this was last verified. Columns were: {list(df.columns)}"
        )

    # Both columns are already 0-100 percentages, one row per user per module —
    # average across modules for a single per-user figure.
    out = df.groupby("user_id").agg(
        ontime_completion=(ontime_col, "mean"),
        overall_completion=(overall_col, "mean"),
    ).reset_index()
    out["assignment_score"] = 0.6 * out["ontime_completion"] + 0.4 * out["overall_completion"]
    return out


# ═══════════════════════════════════════════════════════════════════════════
# MODULE CONTESTS  → module_contest_score (0-100), per module + averaged
# Reuses your existing calculate_total_score() weighting (40% MCQ / 60%
# coding, MCQ-only for EDA-2/Power BI) rather than inventing a new one.
# ═══════════════════════════════════════════════════════════════════════════
def calculate_module_score(row):
    mcq = pd.to_numeric(row.get("MCQ_score"), errors="coerce")
    coding = pd.to_numeric(row.get("Coding_score"), errors="coerce")
    mcq = mcq if pd.notna(mcq) else 0
    coding = coding if pd.notna(coding) else 0
    if row["module_name"] in MCQ_ONLY_MODULES:
        return mcq
    return mcq * 0.4 + coding * 0.6


def build_module_contests(y, m):
    mcq = fetch_card_df(CONTEST_MCQ_CARD, "contest MCQ (8057)", require_user_id=True)
    coding = fetch_card_df(CONTEST_CODING_CARD, "contest coding (6391)", require_user_id=True)
    if "contest_title" in coding.columns:
        coding = coding.rename(columns={"contest_title": "contest_name"})

    merge_keys = [k for k in ["user_id", "student_name", "admin_unit_name", "contest_date", "module_name"]
                  if k in mcq.columns and k in coding.columns]
    df = pd.merge(coding, mcq, on=merge_keys, how="outer")
    df = df.rename(columns={"module_wise_score": "MCQ_score", "per_module_marks": "Coding_score"})
    if "contest_date" in df.columns:
        df = df[in_target_month(df["contest_date"], y, m)]

    df["MCQ_score"] = pd.to_numeric(df.get("MCQ_score"), errors="coerce").fillna(0)
    df["Coding_score"] = pd.to_numeric(df.get("Coding_score"), errors="coerce").fillna(0)
    df["module_score"] = df.apply(calculate_module_score, axis=1)

    module_map_inv = {v: k for k, v in MODULE_NAME_MAP.items()}
    df["module_label"] = df["module_name"].map(module_map_inv)
    df = df[df["module_label"].notna()]  # keep only the 5 modules the Learning Score sheet tracks

    per_module = (
        df.sort_values("module_score", ascending=False)
          .groupby(["user_id", "module_label"])
          .agg(avg_score=("module_score", "mean"), max_score=("module_score", "max"))
          .reset_index()
    )
    overall = per_module.groupby("user_id")["max_score"].mean().reset_index()
    overall = overall.rename(columns={"max_score": "module_contest_score"})

    # NOTE: "no. of hard questions solved" / "hard+medium time spent" from the sheet template
    # aren't in either raw card as fetched (they're per-question, difficulty-tagged rows) — this
    # gives you the score. If you need the per-question difficulty breakdown too, that likely
    # needs a join to the underlying assignment-question table by question id; flag this if so
    # and I'll add it once I can see those cards' native SQL.
    return per_module, overall


# ═══════════════════════════════════════════════════════════════════════════
# PROJECTS  → project_score (0-100)
# ═══════════════════════════════════════════════════════════════════════════
def build_projects(y, m):
    raw_frames = [fetch_card_df(cid, f"projects raw ({cid})") for cid in PROJECTS_RAW_CARDS]
    raw_frames = [f for f in raw_frames if f is not None]
    df = pd.concat(raw_frames, axis=0, ignore_index=True)
    if "User ID" in df.columns:
        df = df.rename(columns={"User ID": "user_id"})
    if "user_id" not in df.columns:
        raise RuntimeError(f"❌ Cards {PROJECTS_RAW_CARDS} have no User ID/user_id column — columns: {list(df.columns)}")

    for c in ("Submission Time", "project_deadline_date"):
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    if "Submission Time" in df.columns:
        df = df[in_target_month(df["Submission Time"], y, m)]

    score_col = next((c for c in df.columns if "marks" in c.lower() or c.lower() == "score"), None)
    if not score_col:
        raise RuntimeError(f"❌ Couldn't find a score/marks column on projects cards — columns: {list(df.columns)}")

    agg = {score_col: ["mean", "median", "count"]}
    grouped = df.groupby("user_id").agg(agg)
    grouped.columns = ["avg_score", "median_score", "attempts_to_clear"]
    grouped = grouped.reset_index()

    if "Submission Time" in df.columns and "project_deadline_date" in df.columns:
        first_sub = df.groupby("user_id")["Submission Time"].min().reset_index(name="first_submission_time")
        grouped = grouped.merge(first_sub, on="user_id", how="left")

    grouped["project_score"] = (
        0.45 * (grouped["avg_score"].clip(0, 100))
        + 0.30 * (100 / grouped["attempts_to_clear"].clip(lower=1)).clip(0, 100)
        + 0.25 * 100  # time-efficiency term left neutral until a per-project benchmark is set — see TODO
    ) / 1.0
    # ⚠ time_efficiency term above is a placeholder (flat 100) — plug in a real
    # benchmark (e.g. median time-to-clear for that project) once you have one.
    return grouped


# ═══════════════════════════════════════════════════════════════════════════
# ARENA / PLAYLIST — STUB. No confirmed source card yet (see docstring).
# ═══════════════════════════════════════════════════════════════════════════
def build_arena(y, m):
    if ARENA_CARD is None:
        print("⚠️  Arena/Playlist has no confirmed card yet — skipping this section "
              "(composite score will renormalize weight across the sections that ARE available).")
        return None
    raise NotImplementedError


# ═══════════════════════════════════════════════════════════════════════════
# TA + GROOMING SESSIONS  → session_score (0-100)
# TA sessions (9251) is fetched here with its own `{{date}}` template-tag
# parameter set to the target month — confirmed to exist on its saved query
# (see TA_SESSIONS_DATE_TAG_ID) — instead of via the blind all-time prefetch,
# same pattern as build_assignments(). Grooming sessions (7577) has no such
# tag as far as I've checked, so it's still fetched all-time and filtered
# client-side below.
# ═══════════════════════════════════════════════════════════════════════════
def build_sessions(y, m):
    ta = fetch_card_df(
        TA_SESSIONS_CARD, "TA sessions (9251, month-scoped)", optional=True,
        parameters=[date_range_param("date", y, m, TA_SESSIONS_DATE_TAG_ID)],
    )
    grooming = fetch_card_df(GROOMING_SESSIONS_CARD, "grooming sessions (7577)", optional=True)

    counts = []
    for df, label in [(ta, "ta_sessions"), (grooming, "grooming_sessions")]:
        if df is None or "user_id" not in df.columns:
            continue
        date_col = next((c for c in df.columns if "date" in c.lower() or "timestamp" in c.lower()), None)
        if date_col:
            df = df[in_target_month(df[date_col], y, m)]
        c = df.groupby("user_id").size().reset_index(name=label)
        counts.append(c)

    if not counts:
        print("⚠️  Neither TA nor grooming session data was usable — session_score will be null for everyone.")
        return None

    out = counts[0]
    for c in counts[1:]:
        out = pd.merge(out, c, on="user_id", how="outer")
    out = out.fillna(0)
    total_col = [c for c in out.columns if c != "user_id"]
    out["sessions_attended"] = out[total_col].sum(axis=1)
    # Expected sessions/month is a placeholder — 4 is a guess (~weekly cadence).
    # Replace with your actual expected cadence once confirmed.
    expected_per_month = 4
    out["session_score"] = (out["sessions_attended"] / expected_per_month * 100).clip(upper=100)
    return out


# ═══════════════════════════════════════════════════════════════════════════
# COMPOSITE SCORE
# ═══════════════════════════════════════════════════════════════════════════
# Category weights, as agreed — tune freely, they're read fresh each run.
WEIGHTS = {
    "attendance_score": 15,
    "assignment_score": 15,
    "module_contest_score": 25,
    "project_score": 20,
    "arena_score": 15,
    "session_score": 10,
}


def compute_composite(row):
    available = {k: w for k, w in WEIGHTS.items() if pd.notna(row.get(k))}
    if not available:
        return np.nan
    total_w = sum(available.values())
    return sum(row[k] * (w / total_w) for k, w in available.items())


# ═══════════════════════════════════════════════════════════════════════════
# WRITE — same rate-limit-tolerant pattern as your existing crons
# ═══════════════════════════════════════════════════════════════════════════
def write_sheet(sheet_key, worksheet_name, df):
    print(f"🔄 Writing sheet tab: {worksheet_name} ({len(df)} rows)")
    for attempt in range(1, 6):
        try:
            time.sleep(2)
            sheet = gc.open_by_key(sheet_key)
            try:
                ws = sheet.worksheet(worksheet_name)
            except gspread.exceptions.WorksheetNotFound:
                ws = sheet.add_worksheet(title=worksheet_name, rows=max(len(df) + 10, 100), cols=max(len(df.columns) + 5, 26))
            ws.clear()
            set_with_dataframe(ws, df, include_index=False, include_column_header=True)
            print(f"✅ Wrote: {worksheet_name}")
            return
        except gspread.exceptions.APIError as e:
            if "RESOURCE_EXHAUSTED" in str(e) or "Quota exceeded" in str(e):
                wait = 60 * attempt
                print(f"⏳ Rate limit hit, waiting {wait}s...")
                time.sleep(wait)
            else:
                print(f"❌ Sheets API error writing {worksheet_name}: {e}")
                raise
    raise RuntimeError(f"❌ Failed to write {worksheet_name} after 5 attempts.")


# ═══════════════════════════════════════════════════════════════════════════
# STUDENT MASTER VIEW + LOOKUP — see the file docstring's 2026-09-23 note.
# Runs once at the very end of a run (not per-month) since it aggregates
# across every month tab that currently exists in the sheet, not just the
# one(s) scored this run.
# ═══════════════════════════════════════════════════════════════════════════
MASTER_VIEW_TAB = "Student Master View"
LOOKUP_TAB = "Student Lookup"
PLACEMENT_CORR_TAB = "Placement Corr"

MONTH_TAB_RE = re.compile(r"^[A-Z][a-z]{2}-\d{4}$")  # e.g. "Aug-2026" — excludes
                                                       # its "... - Module Contests
                                                       # (per module)" companion tab

# Column-name candidates on the "Placement Corr" tab, in priority order —
# confirmed against your actual tab (2026-09-23). Kept as candidate LISTS
# (not single hardcoded names) so a header rename on your end degrades
# gracefully instead of silently breaking. "f" is not a typo — that really
# is the live header on the batch column.
PLACEMENT_CORR_COLS = {
    "user_id":         ["UserID", "User ID", "user_id"],
    "batch":           ["Placement-Pipeline Batch", "Placement Batch", "f", "Batch"],
    "placed":          ["Placed"],
    "placement_month": ["Placement Month"],
    "placeability":    ["Placeability"],
    "persona_updated": ["Persona UPDATED"],
    "persona_raw":     ["Persona"],
    "persona_sai":     ["Persona (Sai sheet)"],
}

_PERSONA_JUNK = {"", "na", "n/a", "#n/a", "data not found", "value could not found", "none"}
_PERSONA_BUCKETS = ("Moonshot", "Excellent", "Good", "Average", "Weak")

MASTER_VIEW_SUB_SCORE_COLS = [
    "attendance_score", "assignment_score", "module_contest_score",
    "project_score", "session_score", "arena_score",
]


def _uid_str(v):
    """Stringify a user_id without float artifacts ('54414.0' -> '54414')."""
    if pd.isna(v):
        return ""
    try:
        return str(int(float(v)))
    except (TypeError, ValueError):
        return str(v).strip()


def col_letter(n):
    """1-indexed column number -> spreadsheet column letter(s) (1->A, 27->AA)."""
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def _find_col(header, candidates):
    """Index (0-based) of the first column in `header` matching any of
    `candidates` (case/whitespace-insensitive, in priority order), else None."""
    normalized = {(h or "").strip().lower(): i for i, h in enumerate(header)}
    for cand in candidates:
        i = normalized.get(cand.strip().lower())
        if i is not None:
            return i
    return None


def _clean_persona(v):
    if v is None:
        return None
    s = str(v).strip()
    if s.lower() in _PERSONA_JUNK:
        return None
    return s


def _persona_bucket(v):
    if not v:
        return None
    low = v.lower()
    for b in _PERSONA_BUCKETS:
        if b.lower() in low:
            return b
    return None


def discover_month_tabs(sheet):
    """Every worksheet in THIS Learning Score sheet that looks like a scored
    month tab ('Aug-2026') — sorted chronologically, oldest first. This is
    what lets the master view stay current on its own as more months get
    scored, with no month list to hand-maintain here."""
    tabs = [ws.title for ws in sheet.worksheets() if MONTH_TAB_RE.match(ws.title)]
    return sorted(tabs, key=lambda t: _parse_year_month(t, "month tab name"))


def _read_tab_values(sheet, tab_name):
    values = sheet.worksheet(tab_name).get_all_values()
    if not values:
        return [], []
    return values[0], values[1:]


def build_student_master_view(sheet, roster):
    """Builds ONE wide table — a row per student, every scored month's
    learning_score, the latest month's sub-scores, and (best-effort)
    placement outcome + persona from the "Placement Corr" tab in THIS sheet.
    Computed in Python/pandas, not spreadsheet formulas. Returns
    (DataFrame, ordered_column_list), or (None, None) if there are no
    scored month tabs yet."""
    print("\n" + "=" * 60)
    print("BUILDING STUDENT MASTER VIEW")
    print("=" * 60)

    month_tabs = discover_month_tabs(sheet)
    if not month_tabs:
        print("⚠️  No scored month tabs found in the sheet yet — skipping master view.")
        return None, None
    print(f"📅 Found {len(month_tabs)} scored month tab(s): {', '.join(month_tabs)}")

    keep = [c for c in ["user_id", "student_name", "email", "au_batch_name", "label", "gem_label"] if c in roster.columns]
    df = roster[keep].copy()
    df = df.rename(columns={"au_batch_name": "batch"})
    df["user_id"] = df["user_id"].apply(_uid_str)

    month_cols = []
    latest_tab, latest_by_uid = None, None
    for tab in month_tabs:
        print(f"  → reading '{tab}'...")
        header, rows = _read_tab_values(sheet, tab)
        if not header or "user_id" not in header:
            print(f"  ⚠️  '{tab}' unreadable or missing user_id — skipped.")
            continue
        mdf = pd.DataFrame(rows, columns=header)
        mdf["user_id"] = mdf["user_id"].apply(_uid_str)
        mdf["learning_score"] = pd.to_numeric(mdf.get("learning_score"), errors="coerce")
        col_name = f"{tab} LS"
        df[col_name] = df["user_id"].map(mdf.set_index("user_id")["learning_score"])
        month_cols.append(col_name)
        latest_tab, latest_by_uid = tab, mdf.set_index("user_id")  # ends on the chronologically-last tab

    if month_cols:
        df["avg_learning_score"] = df[month_cols].mean(axis=1, skipna=True).round(1)
        df["months_with_data"] = df[month_cols].notna().sum(axis=1)
        df["score_trend"] = (df[month_cols[-1]] - df[month_cols[0]]).round(1)
    else:
        df["avg_learning_score"], df["months_with_data"], df["score_trend"] = np.nan, 0, np.nan

    if latest_by_uid is not None:
        for c in MASTER_VIEW_SUB_SCORE_COLS:
            out_col = f"latest ({latest_tab}) {c}"
            df[out_col] = df["user_id"].map(pd.to_numeric(latest_by_uid[c], errors="coerce")) if c in latest_by_uid.columns else np.nan

    # ── Placement Corr — a tab in THIS sheet, see file docstring's 2026-09-23 note ──
    placement_cols = ["placement_status", "placement_month", "placeability",
                       "placement_pipeline_batch", "persona", "persona_bucket"]
    for c in placement_cols:
        df[c] = None
    try:
        pc_values = sheet.worksheet(PLACEMENT_CORR_TAB).get_all_values()
        if not pc_values:
            raise ValueError("tab is empty")
        header, rows = pc_values[0], pc_values[1:]
        idx = {key: _find_col(header, cands) for key, cands in PLACEMENT_CORR_COLS.items()}
        if idx["user_id"] is None:
            raise ValueError(f"couldn't find a UserID column — header started with {header[:10]}")
        uid_i = idx["user_id"]
        pc = {}
        for r in rows:
            if uid_i >= len(r) or not r[uid_i].strip():
                continue
            uid = _uid_str(r[uid_i])

            def get(key, r=r):
                i = idx.get(key)
                return r[i].strip() if i is not None and i < len(r) and r[i] else None

            persona = _clean_persona(get("persona_updated")) or _clean_persona(get("persona_raw")) or _clean_persona(get("persona_sai"))
            pc[uid] = {
                "placement_status": get("placed"),
                "placement_month": get("placement_month"),
                "placeability": get("placeability"),
                "placement_pipeline_batch": get("batch"),
                "persona": persona,
                "persona_bucket": _persona_bucket(persona),
            }
        for c in placement_cols:
            df[c] = df["user_id"].map(lambda u, c=c: pc.get(u, {}).get(c))
        matched = df["user_id"].isin(pc.keys()).sum()
        print(f"✓ Matched {matched}/{len(df)} students against '{PLACEMENT_CORR_TAB}' ({len(pc)} rows had a UserID there).")
    except (gspread.exceptions.WorksheetNotFound, ValueError) as e:
        print(f"⚠️  Couldn't read '{PLACEMENT_CORR_TAB}' tab ({e}) — placement/persona columns "
              f"will be blank in the master view this run. Best-effort only — not fatal.")

    ordered_cols = ["user_id", "student_name", "email", "batch", "label", "gem_label"]
    ordered_cols += month_cols
    ordered_cols += ["avg_learning_score", "months_with_data", "score_trend"]
    if latest_by_uid is not None:
        ordered_cols += [f"latest ({latest_tab}) {c}" for c in MASTER_VIEW_SUB_SCORE_COLS]
    ordered_cols += placement_cols
    ordered_cols = [c for c in ordered_cols if c in df.columns]
    return df[ordered_cols], ordered_cols


def write_lookup_tab(sheet, master_view_df, ordered_cols):
    """Writes the compact 'Student Lookup' tab: two input cells (B3 = batch
    name contains, B4 = user ID equals) and a live FILTER() pulling matching
    rows out of Student Master View underneath. Uses Sheets' native FILTER/
    SEARCH — written directly via gspread into Google Sheets, so (unlike the
    one-off analysis workbook delivered earlier) there's no Excel/LibreOffice
    portability concern here."""
    if master_view_df is None:
        return
    n_rows, n_cols = len(master_view_df), len(ordered_cols)
    last_col = col_letter(n_cols)
    last_row = n_rows + 1  # +1 for Student Master View's own header row
    batch_col = col_letter(ordered_cols.index("batch") + 1) if "batch" in ordered_cols else None
    uid_col = col_letter(ordered_cols.index("user_id") + 1) if "user_id" in ordered_cols else "A"

    print(f"🔄 Writing lookup tab: {LOOKUP_TAB}")
    for attempt in range(1, 6):
        try:
            time.sleep(2)
            try:
                ws = sheet.worksheet(LOOKUP_TAB)
                ws.clear()
            except gspread.exceptions.WorksheetNotFound:
                ws = sheet.add_worksheet(title=LOOKUP_TAB, rows=max(n_rows + 20, 100), cols=max(n_cols + 2, 26))

            conds = []
            if batch_col:
                conds.append(f"(ISNUMBER(SEARCH($B$3,'{MASTER_VIEW_TAB}'!{batch_col}2:{batch_col}{last_row}))+($B$3=\"\"))")
            conds.append(f"((TO_TEXT('{MASTER_VIEW_TAB}'!{uid_col}2:{uid_col}{last_row})=TO_TEXT($B$4))+($B$4=\"\"))")
            filter_formula = (
                f"=IFERROR(FILTER('{MASTER_VIEW_TAB}'!A2:{last_col}{last_row}, {'*'.join(conds)}), "
                f"\"No matches — check the batch name / user ID, or clear both boxes to see everyone.\")"
            )

            rows_to_write = [
                ["STUDENT LOOKUP — fill in either box below (or both, or neither for everyone) and matching rows appear underneath."],
                [],
                ["Batch name contains:", ""],
                ["User ID equals:", ""],
                [],
                ordered_cols,
                [filter_formula],
            ]
            ws.update(range_name="A1", values=rows_to_write, value_input_option="USER_ENTERED")
            print(f"✅ Wrote: {LOOKUP_TAB}")
            return
        except gspread.exceptions.APIError as e:
            if "RESOURCE_EXHAUSTED" in str(e) or "Quota exceeded" in str(e):
                wait = 60 * attempt
                print(f"⏳ Rate limit hit, waiting {wait}s...")
                time.sleep(wait)
            else:
                print(f"❌ Sheets API error writing {LOOKUP_TAB}: {e}")
                raise
    raise RuntimeError(f"❌ Failed to write {LOOKUP_TAB} after 5 attempts.")


# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════
def score_month(y, m, tab_name, roster):
    """Builds and writes the two sheet tabs for ONE month, reusing the
    already-fetched/cached all-time cards (roster, attendance #11636, module
    contest cards, project cards, grooming sessions #7577) and re-fetching
    only the two cards that are scoped server-side per month (assignments
    #7939, TA sessions #9251). Returns the number of users scored."""
    attendance = build_attendance(y, m)
    assignments = build_assignments(y, m)
    per_module_contests, module_contests = build_module_contests(y, m)
    projects = build_projects(y, m)
    arena = build_arena(y, m)
    sessions = build_sessions(y, m)

    df = roster
    for part, cols in [
        (attendance, ["user_id", "no_of_attended", "sessions_in_scope", "avg_time", "attendance_score"]),
        (assignments, ["user_id", "ontime_completion", "overall_completion", "assignment_score"]),
        (module_contests, ["user_id", "module_contest_score"]),
        (projects, ["user_id", "avg_score", "median_score", "attempts_to_clear", "project_score"]),
        (arena, None),
        (sessions, ["user_id", "sessions_attended", "session_score"]),
    ]:
        if part is None:
            continue
        use_cols = [c for c in (cols or part.columns) if c in part.columns]
        df = pd.merge(df, part[use_cols], on="user_id", how="left")

    df["arena_score"] = df.get("arena_score", np.nan)  # stays null until build_arena() is wired
    df["learning_score"] = df.apply(compute_composite, axis=1)
    df["month"] = tab_name

    write_sheet(LEARNING_SCORE_SHEET_KEY, tab_name, df)
    write_sheet(LEARNING_SCORE_SHEET_KEY, f"{tab_name} - Module Contests (per module)", per_module_contests)
    return len(df)


if __name__ == "__main__":
    print("🚀 Starting Learning Score cron...")
    print(f"📅 Start: {datetime.now(IST).strftime('%d-%b-%Y %H:%M:%S IST')}\n")

    try:
        months = target_months()
    except ValueError as e:
        print(f"\n{e}")
        sys.exit(1)

    if len(months) > 1:
        print(f"📦 BACKFILL MODE — scoring {len(months)} months: "
              f"{months[0][2]} through {months[-1][2]}\n")
    else:
        y, m, tab_name = months[0]
        print(f"📆 Scoring month: {calendar.month_name[m]} {y}  →  tab '{tab_name}'\n")

    # Shared setup — fetched/built ONCE regardless of how many months are
    # being scored (see score_month()'s docstring for why this is safe).
    try:
        prefetch_all_cards()
        roster = build_roster()
        print(f"👥 Active roster: {len(roster)} users")
    except Exception:
        print("\n❌ Learning Score cron failed during shared setup:")
        traceback.print_exc()
        sys.exit(1)

    failures = []
    for y, m, tab_name in months:
        if len(months) > 1:
            print("\n" + "=" * 60)
            print(f"📆 Scoring {calendar.month_name[m]} {y}  →  tab '{tab_name}'")
            print("=" * 60)
        try:
            n_users = score_month(y, m, tab_name, roster)
            print(f"✅ {tab_name} done — {n_users} users scored.")
        except Exception:
            print(f"\n❌ {tab_name} failed:")
            traceback.print_exc()
            failures.append(tab_name)
            continue  # one bad month shouldn't sink the rest of a backfill
        if len(months) > 1:
            time.sleep(3)  # be gentle on the Sheets API across many writes

    # Student Master View + Student Lookup — rebuilt fresh every run from
    # whatever month tabs currently exist (not just the ones scored just
    # now). Best-effort: a failure here is logged and does NOT fail the run
    # or affect the exit code — the per-month scoring above is what matters.
    try:
        sheet_obj = gc.open_by_key(LEARNING_SCORE_SHEET_KEY)
        master_df, ordered_cols = build_student_master_view(sheet_obj, roster)
        if master_df is not None:
            write_sheet(LEARNING_SCORE_SHEET_KEY, MASTER_VIEW_TAB, master_df)
            write_lookup_tab(sheet_obj, master_df, ordered_cols)
    except Exception:
        print("\n⚠️  Student Master View / Lookup build failed (non-fatal — "
              "per-month scoring above already succeeded):")
        traceback.print_exc()

    elapsed = time.time() - start_time
    scored = len(months) - len(failures)
    print(f"\n🎯 Done in {int(elapsed // 60)}m {int(elapsed % 60)}s — "
          f"{scored}/{len(months)} month(s) scored successfully.")
    if failures:
        print(f"❌ Failed month(s): {', '.join(failures)}")
        sys.exit(1)
