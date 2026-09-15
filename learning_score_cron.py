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

Everything else (roster, attendance, assignments, module contests, projects,
grooming-session count) is now verified against actual Metabase query
definitions or your two production scripts' real output — see CARD IDS below.
"""
import os
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


def date_range_param(tag_name, y, m):
    """A Metabase `parameters` entry that binds a date/range value to a
    native-query template tag named `tag_name` (e.g. the `{{Date}}` tag on
    card #7939). Pass this in fetch_card(..., parameters=[...]) for any card
    that needs to be scoped to the target month server-side rather than
    filtered client-side after the fact (necessary for cards — like 7939 —
    that return pre-aggregated numbers with no row-level date to filter on)."""
    return {
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
    keep = [c for c in ["user_id", "au_batch_name", "label", "gem_label"] if c in df.columns]
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
        parameters=[date_range_param("Date", y, m)],
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

        elapsed = time.time() - start_time
        print(f"\n🎯 Done in {int(elapsed // 60)}m {int(elapsed % 60)}s — {len(df)} users scored for {tab_name}.")

    except Exception:
        print("\n❌ Learning Score cron failed:")
        traceback.print_exc()
        sys.exit(1)
