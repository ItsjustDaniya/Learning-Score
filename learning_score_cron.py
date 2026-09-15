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
  - Groomers/Master Data sheet ID now wired in
    (13HWMhfMX3i5qsDCEYnQD1h1iFL-ScoNz0HQSgd4CIks) — but I still don't know
    its tab name or column names, so build_placement_tags() falls back to
    the first tab and auto-detects a user_id-like column; it prints every
    column name it finds on each run so you can tell me the real join column
    if "user_id" isn't it. ⚠ Share this sheet with the service account email
    too (see ENV CHECK output) or it won't be readable.
  - Added student_name / email / phone to the output. No new card needed —
    the roster card (#6289) already selects
    concat(first_name,' ',last_name) as student_name, auth_user.email, and
    users_userprofile.phone in its own saved SQL (verified via
    metabase://question/6289 + metabase://table/211/fields — phone lives on
    the "Newton School" DB's users_userprofile table, NOT the Data-Science-DB
    one, which has no phone column at all). build_roster() now keeps those
    columns and they flow straight into the final merged sheet.
  - Added a Placement Correlation step, run at the end of every monthly cron
    run (per your instruction — this is now recurring, not one-off). Pulls
    from the "Placements - FlyWheel" Google Sheet
    (1Ue49enEEpgNaOEdQVgwgsehWvHb3HEI0Q-qekvAzYyU), using BOTH tabs you
    confirmed: "Student_Tags" (one row per user_id, clean "Placement Tag" →
    is_placed) and "Prog<>Placement" (one row per user_id too, wider — adds
    LPA/compensation + placement month for the placed subset). I looked at
    both tabs directly in your browser (you declined sharing this sheet with
    the service account, so I read it visually instead — no other way to get
    a sheet this size in short of that). Since placement is a lagging
    outcome, the correlation uses each student's AVERAGE learning_score
    across every monthly tab this cron has ever written (not just this
    month's snapshot) — see collect_historical_learning_scores(). Writes 3
    new evergreen tabs (overwritten each run, not one per month):
    "Placement Correlation - Summary" (Pearson r of each avg sub-score vs.
    is_placed, and vs. LPA for the placed subset), "... - By Score Bucket"
    (placement rate % per 20-point learning_score band), and
    "... - Detail" (the merged per-user table those are computed from).
    ⚠ I derived STUDENT_TAGS_TAB/PROG_PLACEMENT_TAB's exact columns from a
    live look at the sheet on 2026-09-15, not a schema I can re-verify
    programmatically — build_student_tags()/build_prog_placement_detail()
    print every column name they find each run, and this whole step is
    wrapped so it can't fail the core Learning Score write if the sheet's
    layout ever changes. ⚠ This sheet MUST be shared (at least Viewer) with
    the service account email (see ENV CHECK) or every read here returns
    nothing — silently, since this step is non-fatal by design.

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
  4. Placement Profile Tags — sheet ID is wired (see above) but tab/column
     names are still unverified; check the "📋 Groomers sheet tab..." log
     line on the next run and tell me if the auto-detected join column is
     wrong.
  5. Placement Correlation (new) — STUDENT_TAGS_TAB/PROG_PLACEMENT_TAB column
     names were read visually, not verified against a live query/json pull
     like everything else in this file, since the sheet was never shared
     with the service account for me to hit programmatically. Share it (see
     PLACEMENTS_SHEET_KEY block above) and check the "📋 '...' tab: ... rows,
     columns: [...]" log lines on the next run — if is_placed / lpa /
     placement_month come back empty, paste me those column lists and I'll
     fix the lookups.

Everything else (roster, attendance, assignments, module contests, projects,
grooming-session count) is now verified against actual Metabase query
definitions or your two production scripts' real output — see CARD IDS below.
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

# "Groomers and Master Data 2026" sheet:
# https://docs.google.com/spreadsheets/d/13HWMhfMX3i5qsDCEYnQD1h1iFL-ScoNz0HQSgd4CIks/
# Defaulted here too, same as the Learning Score sheet — still overridable.
# ⚠ Also needs to be shared (at least Viewer) with the service account email.
# GROOMERS_SHEET_TAB is intentionally left unset by default: I don't know the
# real tab name, so build_placement_tags() falls back to "whichever tab is
# first" rather than guessing a name that might not exist. Set
# GROOMERS_SHEET_TAB explicitly once you know which tab holds the data.
DEFAULT_GROOMERS_SHEET_KEY = "13HWMhfMX3i5qsDCEYnQD1h1iFL-ScoNz0HQSgd4CIks"
GROOMERS_SHEET_KEY = os.getenv("GROOMERS_SHEET_KEY", DEFAULT_GROOMERS_SHEET_KEY)
GROOMERS_SHEET_TAB = os.getenv("GROOMERS_SHEET_TAB")  # None = use the first tab

# "Placements - FlyWheel" sheet:
# https://docs.google.com/spreadsheets/d/1Ue49enEEpgNaOEdQVgwgsehWvHb3HEI0Q-qekvAzYyU/
# Used for the Placement Correlation step at the end of the run. Two tabs,
# confirmed by looking at the sheet directly (2026-09-15) — both are already
# one row per user_id, no de-dup needed:
#   - Student_Tags: clean "Placement Tag" column (Placed / Current enrolled /
#     Not In grooming / Refund Request / ...) → the primary is_placed signal.
#   - Prog<>Placement: much wider (~60 cols); adds LPA (compensation) and
#     Placement Month for the placed subset, plus its own free-text "Placed"
#     status as a cross-check. ⚠ Its real header row is ROW 2, not row 1 (row
#     1 has merged section-group labels + a stray #REF! cell) — handled in
#     build_prog_placement_detail(), not with the usual get_all_records().
# ⚠ Share this sheet (at least Viewer) with the service account email too
#   (see ENV CHECK) or the Placement Correlation tabs will just come back
#   empty — this step is intentionally non-fatal, so a run without sharing
#   still writes the core Learning Score tab fine, it just skips these 3.
DEFAULT_PLACEMENTS_SHEET_KEY = "1Ue49enEEpgNaOEdQVgwgsehWvHb3HEI0Q-qekvAzYyU"
PLACEMENTS_SHEET_KEY = os.getenv("PLACEMENTS_SHEET_KEY", DEFAULT_PLACEMENTS_SHEET_KEY)
STUDENT_TAGS_TAB = os.getenv("STUDENT_TAGS_TAB", "Student_Tags")
PROG_PLACEMENT_TAB = os.getenv("PROG_PLACEMENT_TAB", "Prog<>Placement")

# "previous" (default) scores last calendar month — run this on/after the
# 1st and it scores the month that just ended. "current" scores month-to-date.
RUN_MONTH_MODE = os.getenv("RUN_MONTH_MODE", "previous")

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
print(f"   SA client_email    : {service_info.get('client_email')}  (share both Sheets with this)")
print(f"   Learning Score sheet: {LEARNING_SCORE_SHEET_KEY}")
print(f"   Groomers sheet      : {GROOMERS_SHEET_KEY} (tab: {GROOMERS_SHEET_TAB or '[first tab]'})")
print(f"   Placements sheet    : {PLACEMENTS_SHEET_KEY} (tabs: '{STUDENT_TAGS_TAB}' + '{PROG_PLACEMENT_TAB}')")

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

PROJECTS_RAW_CARDS = (6241, 6242)  # Confirmed: Data Pipeline "Projects Raw" — user-level, has
                                    # Submission Time / marks_obtained / project_deadline_date.
PROJECT_EVAL_CARDS = (6578, 6579)  # Confirmed: Data Pipeline "Project Evaluations".

GROOMING_SESSIONS_CARD = 7577      # From the DS Learning Queries collection — actively maintained
                                    # (refactor notes dated this month). Not in either existing
                                    # pipeline, but is the only real candidate found.

TA_SESSIONS_CARD = 9251            # ⚠ BEST GUESS — not used by either existing pipeline. Confirm this
                                    # is really "1:1 sessions a student attended" and not something else.

ARENA_CARD = None                  # ⚠ NOT IDENTIFIED — see build_arena() below.

# ASSIGNMENTS_CARD is deliberately excluded — it's fetched separately, with a
# month-scoped Date parameter, inside build_assignments().
REQUIRED_CARDS = [
    ROSTER_CARD, CONTEST_MCQ_CARD, CONTEST_CODING_CARD,
    ATTENDANCE_CARD,
    *PROJECTS_RAW_CARDS, *PROJECT_EVAL_CARDS,
    GROOMING_SESSIONS_CARD, TA_SESSIONS_CARD,
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
def target_month():
    """Returns (year, month, tab_name) for whichever month this run scores."""
    now = datetime.now(IST)
    if RUN_MONTH_MODE == "current":
        y, m = now.year, now.month
    else:  # "previous"
        first_of_this_month = now.replace(day=1)
        last_month_end = first_of_this_month - timedelta(days=1)
        y, m = last_month_end.year, last_month_end.month
    tab_name = f"{calendar.month_abbr[m]}-{y}"
    return y, m, tab_name


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
# ═══════════════════════════════════════════════════════════════════════════
def build_sessions(y, m):
    ta = fetch_card_df(TA_SESSIONS_CARD, "TA sessions (9251)", optional=True)
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
# PLACEMENT PROFILE TAGS — from the Groomers/Master Data sheet, once wired.
# Reference columns only; NOT part of the numeric composite.
# ═══════════════════════════════════════════════════════════════════════════
def build_placement_tags():
    if not GROOMERS_SHEET_KEY:
        return None
    try:
        sheet = gc.open_by_key(GROOMERS_SHEET_KEY)
        # No confirmed tab name yet — use the tab explicitly set via
        # GROOMERS_SHEET_TAB if you've set one, otherwise whichever tab is
        # first, rather than guessing a name that might not exist.
        ws = sheet.worksheet(GROOMERS_SHEET_TAB) if GROOMERS_SHEET_TAB else sheet.get_worksheet(0)
        records = ws.get_all_records()
        df = pd.DataFrame(records)
        print(f"📋 Groomers sheet tab '{ws.title}': {len(df)} rows, columns: {list(df.columns)}")
        user_col = next((c for c in df.columns if c.lower().replace(" ", "_") in ("user_id", "userid")), None)
        if user_col is None:
            print(f"⚠️  No obvious user_id column in tab '{ws.title}' — columns were: {list(df.columns)}. "
                  f"Tell me the real join column (user_id? email? student name?) and I'll wire it up.")
            return None
        if user_col != "user_id":
            df = df.rename(columns={user_col: "user_id"})
        return df
    except Exception as e:
        print(f"⚠️  Could not read Groomers sheet: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════════════
# PLACEMENT CORRELATION — pulls from the "Placements - FlyWheel" sheet and
# correlates each learning-score sub-metric against placement outcomes.
# Entirely optional/non-fatal: every function here returns None on any
# problem rather than raising, and the __main__ block wraps the whole step
# in a try/except so a bad column name or an unshared sheet can never take
# down the core Learning Score write.
# ═══════════════════════════════════════════════════════════════════════════
def _dedupe_header(header):
    """Turns a raw header row into unique column names — needed for the
    Prog<>Placement tab, whose row 2 (its real header) has a few blank cells
    (collapsed/grouped columns) that would otherwise collide as ''."""
    seen = {}
    out = []
    for h in header:
        h = (h or "").strip() or "_blank"
        if h in seen:
            seen[h] += 1
            h = f"{h}_{seen[h]}"
        else:
            seen[h] = 0
        out.append(h)
    return out


def _find_col(norm_map, substr):
    """norm_map: {original_col_name: normalized_lowercase_no_space_name}.
    Returns the first original column name whose normalized form contains
    `substr`, or None."""
    for c, n in norm_map.items():
        if substr in n:
            return c
    return None


def build_student_tags():
    """Student_Tags tab of the Placements sheet — one row per user_id, with
    a clean "Placement Tag" column (Placed / Current enrolled / Not In
    grooming / Refund Request / ...). This is the primary is_placed signal —
    confirmed by eye to already be one row per student, standard header on
    row 1, so get_all_records() works as-is (unlike Prog<>Placement below)."""
    if not PLACEMENTS_SHEET_KEY:
        return None
    try:
        sheet = gc.open_by_key(PLACEMENTS_SHEET_KEY)
        ws = sheet.worksheet(STUDENT_TAGS_TAB)
        records = ws.get_all_records()
        if not records:
            print(f"⚠️  '{STUDENT_TAGS_TAB}' tab is empty.")
            return None
        df = pd.DataFrame(records)
        print(f"📋 '{STUDENT_TAGS_TAB}' tab: {len(df)} rows, columns: {list(df.columns)}")

        norm = {c: str(c).strip().lower().replace(" ", "").replace("_", "") for c in df.columns}
        user_col = _find_col(norm, "userid")
        tag_col = _find_col(norm, "placementtag")
        if user_col is None:
            print(f"⚠️  No user_id column found in '{STUDENT_TAGS_TAB}' — columns were: {list(df.columns)}. Skipping.")
            return None

        out = pd.DataFrame({"user_id": pd.to_numeric(df[user_col], errors="coerce")})
        out = out[out["user_id"].notna()].copy()
        out["user_id"] = out["user_id"].astype(int)

        if tag_col:
            tag = df.loc[out.index, tag_col].astype(str).str.strip()
            out["placement_tag"] = tag
            out["is_placed"] = (tag.str.lower() == "placed").astype(int)
        else:
            print(f"⚠️  No 'Placement Tag' column found in '{STUDENT_TAGS_TAB}' — columns were: {list(df.columns)}.")

        for label, substr in [
            ("eligibility_level", "eligibilitylevel"),
            ("phase", "phase"),
            ("enrolled_status", "enrolledstatus"),
        ]:
            c = _find_col(norm, substr)
            if c:
                out[label] = df.loc[out.index, c]

        return out.drop_duplicates(subset="user_id")
    except gspread.exceptions.WorksheetNotFound:
        print(f"⚠️  Tab '{STUDENT_TAGS_TAB}' not found in Placements sheet (checked STUDENT_TAGS_TAB) — skipping is_placed.")
        return None
    except Exception as e:
        print(f"⚠️  Could not read '{STUDENT_TAGS_TAB}' tab: {e}")
        return None


def build_prog_placement_detail():
    """Prog<>Placement tab of the Placements sheet — also one row per
    user_id (confirmed by eye, ~3,661 rows), but its real header is on ROW 2,
    not row 1 (row 1 has merged section-group labels like "Grooming"/"PR"/
    "Placement"/"Debarred" plus a stray #REF! cell), so get_all_records()
    (which assumes row 1) would silently misread it — this reads raw values
    and builds the header from row 2 itself. Adds LPA (compensation) and
    Placement Month for the placed subset, plus its own free-text "Placed"
    status column (e.g. "Placed - NS ...", "Placed - Self (...)", "Placed
    once; now returned") as a cross-check against Student_Tags."""
    if not PLACEMENTS_SHEET_KEY:
        return None
    try:
        sheet = gc.open_by_key(PLACEMENTS_SHEET_KEY)
        ws = sheet.worksheet(PROG_PLACEMENT_TAB)
        values = ws.get_all_values()
        if len(values) < 3:
            print(f"⚠️  '{PROG_PLACEMENT_TAB}' tab has too few rows (header expected on row 2) — skipping.")
            return None
        header = _dedupe_header(values[1])
        rows = [r + [""] * (len(header) - len(r)) for r in values[2:]]  # pad short rows
        df = pd.DataFrame(rows, columns=header)
        print(f"📋 '{PROG_PLACEMENT_TAB}' tab: {len(df)} rows, columns: {list(df.columns)}")

        norm = {c: str(c).strip().lower().replace(" ", "") for c in df.columns}
        user_col = _find_col(norm, "userid")
        placed_col = _find_col(norm, "placed")
        lpa_col = _find_col(norm, "lpa")
        month_col = _find_col(norm, "placementmonth")

        if user_col is None:
            print(f"⚠️  No UserID column found in '{PROG_PLACEMENT_TAB}' — columns were: {list(df.columns)}. Skipping.")
            return None

        out = pd.DataFrame({"user_id": pd.to_numeric(df[user_col], errors="coerce")})
        out = out[out["user_id"].notna()].copy()
        out["user_id"] = out["user_id"].astype(int)

        if placed_col:
            placed_text = df.loc[out.index, placed_col].astype(str).str.strip()
            out["placed_status_detail"] = placed_text
            out["is_placed_detail"] = placed_text.str.lower().str.startswith("placed").astype(int)
        if lpa_col:
            out["lpa"] = pd.to_numeric(df.loc[out.index, lpa_col], errors="coerce")
        if month_col:
            out["placement_month"] = df.loc[out.index, month_col]

        if not any([placed_col, lpa_col, month_col]):
            print(f"⚠️  Found no Placed/LPA/Placement Month columns in '{PROG_PLACEMENT_TAB}' — "
                  f"columns were: {list(df.columns)}. Tell me the real names and I'll fix the lookup.")

        return out.drop_duplicates(subset="user_id")
    except gspread.exceptions.WorksheetNotFound:
        print(f"⚠️  Tab '{PROG_PLACEMENT_TAB}' not found in Placements sheet (checked PROG_PLACEMENT_TAB) — skipping LPA/placement month.")
        return None
    except Exception as e:
        print(f"⚠️  Could not read '{PROG_PLACEMENT_TAB}' tab: {e}")
        return None


def build_placement_outcomes():
    """Combines both Placements-sheet tabs, per your instruction to use both:
    Student_Tags for the primary is_placed flag, Prog<>Placement for LPA +
    placement month (and a cross-check status). Joined on user_id."""
    tags = build_student_tags()
    detail = build_prog_placement_detail()
    if tags is None and detail is None:
        return None
    if tags is None:
        return detail
    if detail is None:
        return tags
    out = pd.merge(tags, detail, on="user_id", how="outer")
    if "is_placed" in out.columns and "is_placed_detail" in out.columns:
        # Trust Student_Tags' cleaner flag; fall back to the Prog<>Placement
        # text-status flag only for users Student_Tags didn't have.
        out["is_placed"] = out["is_placed"].fillna(out["is_placed_detail"])
    elif "is_placed_detail" in out.columns:
        out["is_placed"] = out["is_placed_detail"]
    return out


def collect_historical_learning_scores():
    """Reads every monthly Learning Score tab this cron has ever written
    (e.g. "Sep-2026") back out of the sheet and averages each user's scores
    across all months they appear in. Placement is a lagging, slow-changing
    outcome — correlating it against a single month's snapshot score would be
    noisy, so this uses the fullest history available (every month run so
    far, including the one this run just wrote) rather than just this run's
    numbers. Module-Contests-per-module tabs and the Placement Correlation
    tabs themselves are excluded by the tab-name pattern (month tabs are
    exactly "Mon-YYYY", nothing else matches)."""
    sheet = gc.open_by_key(LEARNING_SCORE_SHEET_KEY)
    month_tab_re = re.compile(r"^[A-Za-z]{3}-\d{4}$")
    frames = []
    for ws in sheet.worksheets():
        if not month_tab_re.match(ws.title):
            continue
        try:
            records = ws.get_all_records()
        except Exception as e:
            print(f"⚠️  Could not read tab '{ws.title}' for history: {e}")
            continue
        if not records:
            continue
        df = pd.DataFrame(records)
        if "user_id" not in df.columns:
            continue
        df["_source_tab"] = ws.title
        frames.append(df)

    if not frames:
        print("⚠️  No monthly Learning Score tabs found yet — can't build placement-correlation history.")
        return None

    all_months = pd.concat(frames, axis=0, ignore_index=True, sort=False)
    all_months["user_id"] = pd.to_numeric(all_months["user_id"], errors="coerce")
    all_months = all_months[all_months["user_id"].notna()].copy()
    all_months["user_id"] = all_months["user_id"].astype(int)

    score_cols = [c for c in [
        "attendance_score", "assignment_score", "module_contest_score",
        "project_score", "arena_score", "session_score", "learning_score",
    ] if c in all_months.columns]
    for c in score_cols:
        all_months[c] = pd.to_numeric(all_months[c], errors="coerce")

    agg = all_months.groupby("user_id")[score_cols].mean().reset_index()
    agg = agg.rename(columns={c: f"avg_{c}" for c in score_cols})

    months_seen = all_months.groupby("user_id")["_source_tab"].nunique().reset_index(name="months_tracked")
    agg = agg.merge(months_seen, on="user_id", how="left")

    name_col = next((c for c in ["student_name", "email"] if c in all_months.columns), None)
    if name_col:
        first_name = all_months.groupby("user_id")[name_col].first().reset_index()
        agg = agg.merge(first_name, on="user_id", how="left")

    print(f"📊 Learning Score history: {len(agg)} users across {len(frames)} monthly tab(s).")
    return agg


def compute_placement_correlation(history_df, outcomes_df):
    """Returns (summary, score_bucket_pivot, detail) — all None if either
    input is missing/empty or there's no overlap between the two sheets'
    user_ids. summary = Pearson r of each avg_* sub-score against is_placed
    (0/1 — mathematically identical to point-biserial correlation for a
    binary variable, so no extra stats dependency needed) and, for the
    placed subset, against LPA. score_bucket_pivot = placement rate % per
    20-point avg_learning_score band, the plainest "does a higher score mean
    a better placement shot" read of the same data."""
    if history_df is None or outcomes_df is None:
        print("⚠️  Skipping placement correlation — missing Learning Score history or Placements data.")
        return None, None, None

    detail = pd.merge(history_df, outcomes_df, on="user_id", how="inner")
    if detail.empty:
        print("⚠️  No overlapping user_ids between Learning Score history and the Placements sheet — nothing to correlate.")
        return None, None, None
    if "is_placed" not in detail.columns:
        print("⚠️  No is_placed column resolved from the Placements sheet — can't compute correlation.")
        return None, None, detail

    detail["is_placed"] = pd.to_numeric(detail["is_placed"], errors="coerce")
    metric_cols = [c for c in detail.columns if c.startswith("avg_")]

    rows = []
    for col in metric_cols:
        s = pd.to_numeric(detail[col], errors="coerce")
        row = {"metric": col}
        pair = pd.concat([s, detail["is_placed"]], axis=1).dropna()
        row["n_vs_placed"] = len(pair)
        row["corr_vs_is_placed"] = pair.iloc[:, 0].corr(pair.iloc[:, 1]) if len(pair) > 2 else np.nan
        if "lpa" in detail.columns:
            placed_mask = detail["is_placed"] == 1
            pair2 = pd.concat([s[placed_mask], detail.loc[placed_mask, "lpa"]], axis=1).dropna()
            row["n_vs_lpa"] = len(pair2)
            row["corr_vs_lpa"] = pair2.iloc[:, 0].corr(pair2.iloc[:, 1]) if len(pair2) > 2 else np.nan
        rows.append(row)
    summary = pd.DataFrame(rows)

    bucket_df = pd.DataFrame()
    if "avg_learning_score" in detail.columns:
        bins = [0, 20, 40, 60, 80, 100.0001]
        labels = ["0-20", "20-40", "40-60", "60-80", "80-100"]
        detail["score_bucket"] = pd.cut(
            pd.to_numeric(detail["avg_learning_score"], errors="coerce"), bins=bins, labels=labels, right=False
        )
        bucket_rows = []
        for bucket, g in detail.groupby("score_bucket", observed=False):
            n = len(g)
            n_placed = int(g["is_placed"].sum()) if n else 0
            avg_lpa = (
                pd.to_numeric(g.loc[g["is_placed"] == 1, "lpa"], errors="coerce").mean()
                if "lpa" in g.columns else np.nan
            )
            bucket_rows.append({
                "score_bucket": str(bucket),
                "n_students": n,
                "n_placed": n_placed,
                "placement_rate_pct": round(100 * n_placed / n, 1) if n else np.nan,
                "avg_lpa_if_placed": round(avg_lpa, 2) if pd.notna(avg_lpa) else np.nan,
            })
        bucket_df = pd.DataFrame(bucket_rows)

    return summary, bucket_df, detail


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
# MAIN
# ═══════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("🚀 Starting Learning Score cron...")
    print(f"📅 Start: {datetime.now(IST).strftime('%d-%b-%Y %H:%M:%S IST')}\n")

    y, m, tab_name = target_month()
    print(f"📆 Scoring month: {calendar.month_name[m]} {y}  →  tab '{tab_name}'\n")

    try:
        prefetch_all_cards()

        roster = build_roster()
        print(f"👥 Active roster: {len(roster)} users")

        attendance = build_attendance(y, m)
        assignments = build_assignments(y, m)
        per_module_contests, module_contests = build_module_contests(y, m)
        projects = build_projects(y, m)
        arena = build_arena(y, m)
        sessions = build_sessions(y, m)
        placement_tags = build_placement_tags()

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

        if placement_tags is not None:
            df = pd.merge(df, placement_tags, on="user_id", how="left", suffixes=("", "_placement"))

        write_sheet(LEARNING_SCORE_SHEET_KEY, tab_name, df)
        write_sheet(LEARNING_SCORE_SHEET_KEY, f"{tab_name} - Module Contests (per module)", per_module_contests)

        # ─── Placement correlation — recurring, evergreen tabs (not per-month) ───
        # Non-fatal by design: a problem here (sheet not shared, tab renamed,
        # etc.) is printed as a warning, never fails the run — the core
        # Learning Score tab above is already safely written by this point.
        try:
            history = collect_historical_learning_scores()
            outcomes = build_placement_outcomes()
            corr_summary, corr_buckets, corr_detail = compute_placement_correlation(history, outcomes)
            if corr_summary is not None and not corr_summary.empty:
                write_sheet(LEARNING_SCORE_SHEET_KEY, "Placement Correlation - Summary", corr_summary)
            if corr_buckets is not None and not corr_buckets.empty:
                write_sheet(LEARNING_SCORE_SHEET_KEY, "Placement Correlation - By Score Bucket", corr_buckets)
            if corr_detail is not None and not corr_detail.empty:
                write_sheet(LEARNING_SCORE_SHEET_KEY, "Placement Correlation - Detail", corr_detail)
        except Exception:
            print("\n⚠️  Placement correlation step failed (Learning Score tabs above were still written OK):")
            traceback.print_exc()

        elapsed = time.time() - start_time
        print(f"\n🎯 Done in {int(elapsed // 60)}m {int(elapsed % 60)}s — {len(df)} users scored for {tab_name}.")

    except Exception:
        print("\n❌ Learning Score cron failed:")
        traceback.print_exc()
        sys.exit(1)
