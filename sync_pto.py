#!/usr/bin/env python3
"""Sync approved PTO from a Slack channel to Zendesk WFM schedules.

Reads PTO messages from a private Slack channel in the format:
    Name: <full name>
    Email: <email>
    PTO Start: <Month DD, YYYY>
    PTO End: <Month DD, YYYY>

Then creates matching time-off entries in Zendesk WFM.
"""

import argparse
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN", "")
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID", "C0AJQ4ZANCW")

ZENDESK_SUBDOMAIN = os.getenv("ZENDESK_SUBDOMAIN", "atlascard")
ZENDESK_EMAIL = os.getenv("ZENDESK_EMAIL", "")
ZENDESK_API_TOKEN = os.getenv("ZENDESK_API_TOKEN", "")
SYNC_DAYS_AHEAD = int(os.getenv("SYNC_DAYS_AHEAD", "90"))
LOCAL_TZ = ZoneInfo(os.getenv("TIMEZONE", "America/New_York"))

ZENDESK_WFM_BASE = f"https://{ZENDESK_SUBDOMAIN}.zendesk.com/wfm/public/api"
ZENDESK_SUPPORT_BASE = f"https://{ZENDESK_SUBDOMAIN}.zendesk.com/api/v2"

# Zendesk WFM time-off reason IDs (from /v1/timeOffReasons)
DEFAULT_REASON_ID = "43d7520d-3fe3-4a58-ae8e-0b299c796700"  # Paid Leave
SICK_REASON_ID = "d9943065-b864-429e-b6df-2e03641ed846"      # Sick Leave


def log(msg, verbose_only=False, *, verbose=False):
    if verbose_only and not verbose:
        return
    print(msg)


# ---------------------------------------------------------------------------
# Slack API
# ---------------------------------------------------------------------------
def slack_headers():
    return {
        "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        "Content-Type": "application/json",
    }


def fetch_slack_messages(lookback_days: int = 1, verbose=False):
    """Fetch recent messages from the PTO Slack channel.

    Args:
        lookback_days: How many days back to read messages (default 1 = today only).
    """
    oldest_ts = str(
        int((datetime.now(timezone.utc) - timedelta(days=lookback_days)).timestamp())
    )
    all_messages = []
    cursor = None

    while True:
        params = {
            "channel": SLACK_CHANNEL_ID,
            "limit": 100,
            "oldest": oldest_ts,
        }
        if cursor:
            params["cursor"] = cursor

        log(f"  Fetching Slack messages (oldest={oldest_ts})...", verbose_only=True, verbose=verbose)
        resp = requests.get(
            "https://slack.com/api/conversations.history",
            headers=slack_headers(),
            params=params,
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()

        if not data.get("ok"):
            error = data.get("error", "unknown")
            raise RuntimeError(f"Slack API error: {error}")

        messages = data.get("messages", [])
        all_messages.extend(messages)

        cursor = data.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            break

    log(f"  Fetched {len(all_messages)} Slack message(s)", verbose_only=True, verbose=verbose)
    return all_messages


def slack_react(channel: str, timestamp: str, emoji: str, verbose=False):
    """Add a reaction to a Slack message. Silently ignores 'already_reacted'."""
    resp = requests.post(
        "https://slack.com/api/reactions.add",
        headers=slack_headers(),
        json={"channel": channel, "timestamp": timestamp, "name": emoji},
        timeout=10,
    )
    data = resp.json()
    if not data.get("ok"):
        error = data.get("error", "unknown")
        if error == "already_reacted":
            log(f"    Already reacted {emoji} on {timestamp}", verbose_only=True, verbose=verbose)
            return
        log(f"    Slack reaction error: {error}")
    else:
        log(f"    Reacted :{emoji}: on {timestamp}", verbose_only=True, verbose=verbose)


SICK_KEYWORDS = {"sick", "illness", "medical", "doctor", "flu", "covid", "unwell"}


def parse_pto_message(message: dict) -> dict | None:
    """Parse a Slack PTO message into a structured dict.

    Expected format:
        Name: Gabe Whitlatch
        Email: gabe@atlascard.com
        PTO Start: March 18, 2026
        PTO End: March 18, 2026
        Type: Sick          (optional — defaults to PTO)

    Returns dict with keys: name, email, start_date, end_date, reason_id, slack_ts
    or None if the message doesn't match.
    """
    text = message.get("text", "")

    # Skip system messages (channel_join, etc.)
    if message.get("subtype"):
        return None

    # Some apps (e.g. Rippling) put content in blocks, not the text field
    for block in message.get("blocks", []):
        block_text = block.get("text", {}).get("text", "")
        if block_text:
            text = text + "\n" + block_text

    # Clean Slack email formatting: <mailto:x@y.com|x@y.com> → x@y.com
    text = re.sub(r"<mailto:[^|]+\|([^>]+)>", r"\1", text)

    # Extract fields (support both "PTO Start" and Rippling's "Start Date")
    name_match = re.search(r"Name:\s*(.+)", text)
    email_match = re.search(r"Email:\s*(\S+@\S+)", text)
    start_match = re.search(r"(?:PTO Start|Start Date):\s*(.+)", text)
    end_match = re.search(r"(?:PTO End|End Date):\s*(.+)", text)
    type_match = re.search(r"(?:Type|Reason):\s*(.+)", text)

    if not (email_match and start_match and end_match):
        return None

    name = name_match.group(1).strip() if name_match else "Unknown"
    email = email_match.group(1).strip().lower()

    # Parse dates like "March 18, 2026"
    start_date = _parse_human_date(start_match.group(1).strip())
    end_date = _parse_human_date(end_match.group(1).strip())

    if not start_date or not end_date:
        return None

    # Detect sick leave from explicit Type field or keyword scan
    reason_id = DEFAULT_REASON_ID
    if type_match:
        leave_type = type_match.group(1).strip().lower()
        if any(kw in leave_type for kw in SICK_KEYWORDS):
            reason_id = SICK_REASON_ID
    else:
        text_lower = text.lower()
        if any(kw in text_lower for kw in SICK_KEYWORDS):
            reason_id = SICK_REASON_ID

    return {
        "name": name,
        "email": email,
        "start_date": start_date,
        "end_date": end_date,
        "reason_id": reason_id,
        "slack_ts": message.get("ts", ""),
    }


def _parse_human_date(date_str: str) -> str | None:
    """Parse 'March 18, 2026' → '2026-03-18'. Returns None on failure."""
    for fmt in ("%B %d, %Y", "%B %d %Y", "%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


# ---------------------------------------------------------------------------
# Zendesk Support API (user lookup)
# ---------------------------------------------------------------------------
def zendesk_auth():
    return (f"{ZENDESK_EMAIL}/token", ZENDESK_API_TOKEN)


def build_wfm_agent_map(shifts_by_agent: dict, verbose=False) -> dict:
    """Build email → WFM agent ID mapping by looking up each WFM agent in Support API.

    Also indexes by name (lowercase) for fallback matching.
    Returns dict with 'by_email' and 'by_name' sub-dicts.
    """
    by_email = {}
    by_name = {}

    for agent_id in shifts_by_agent:
        resp = requests.get(
            f"{ZENDESK_SUPPORT_BASE}/users/{agent_id}.json",
            auth=zendesk_auth(),
            timeout=15,
        )
        if resp.status_code != 200:
            continue
        user = resp.json().get("user", {})
        email = user.get("email", "").lower()
        name = user.get("name", "").lower()

        if email:
            by_email[email] = agent_id
        if name:
            by_name[name] = agent_id

    log(f"  Built WFM agent map: {len(by_email)} agents", verbose_only=True, verbose=verbose)
    return {"by_email": by_email, "by_name": by_name}


def resolve_email_to_agent_id(email: str, cache: dict, verbose=False):
    """Look up a Zendesk user ID by email. Returns agent_id or None."""
    if email in cache:
        return cache[email]

    resp = requests.get(
        f"{ZENDESK_SUPPORT_BASE}/users/search.json",
        params={"query": email},
        auth=zendesk_auth(),
        timeout=15,
    )
    resp.raise_for_status()
    users = resp.json().get("users", [])

    for user in users:
        if user.get("email", "").lower() == email.lower():
            agent_id = user["id"]
            cache[email] = agent_id
            log(f"    Resolved {email} → agent {agent_id}", verbose_only=True, verbose=verbose)
            return agent_id

    cache[email] = None
    log(f"    Could not resolve {email} in Zendesk", verbose_only=True, verbose=verbose)
    return None


# ---------------------------------------------------------------------------
# Zendesk WFM API
# ---------------------------------------------------------------------------
def fetch_zendesk_shifts(start_date: str, end_date: str, verbose=False):
    """Fetch all shifts in the date range. Returns {agent_id: [{startTime, endTime}, ...]}."""
    shifts_by_agent = {}
    page = 1

    while True:
        body = {
            "startDate": start_date,
            "endDate": end_date,
            "orderBy": {"column": "startTime", "direction": "asc"},
            "page": page,
        }
        log(f"  Fetching Zendesk WFM shifts (page={page})...", verbose_only=True, verbose=verbose)
        resp = requests.post(
            f"{ZENDESK_WFM_BASE}/v1/shifts/fetch",
            json=body,
            auth=zendesk_auth(),
            timeout=30,
        )
        resp.raise_for_status()
        result = resp.json()
        data = result.get("data", [])

        if not data:
            break

        for shift in data:
            aid = shift["agentId"]
            shifts_by_agent.setdefault(aid, []).append({
                "startTime": shift["startTime"],
                "endTime": shift["endTime"],
            })

        meta = result.get("metadata", {})
        total_pages = meta.get("total", 1)
        if page >= total_pages:
            break
        page += 1

    agent_count = len(shifts_by_agent)
    shift_count = sum(len(v) for v in shifts_by_agent.values())
    log(f"  Found {shift_count} shift(s) for {agent_count} agent(s)", verbose_only=True, verbose=verbose)
    return shifts_by_agent


def fetch_existing_timeoff(start_date: str, end_date: str, verbose=False):
    """Fetch existing time-off entries. Returns set of (agent_id, start_time, end_time)."""
    existing = set()
    start_ts = int(datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
    end_ts = int(datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()) + 86400
    page = 1

    while True:
        params = {
            "startTime": start_ts,
            "endTime": end_ts,
            "perPage": 50,
            "page": page,
        }
        log(f"  Fetching existing Zendesk time-off (page={page})...", verbose_only=True, verbose=verbose)
        resp = requests.get(
            f"{ZENDESK_WFM_BASE}/v1/timeOff",
            params=params,
            auth=zendesk_auth(),
            timeout=15,
        )
        resp.raise_for_status()
        result = resp.json()
        data = result.get("data", [])

        for entry in data:
            existing.add((entry["agentId"], entry["startTime"], entry["endTime"]))

        meta = result.get("metadata", {})
        next_page = meta.get("next")
        if not next_page or not data:
            break
        page += 1

    log(f"  Found {len(existing)} existing time-off entry/entries", verbose_only=True, verbose=verbose)
    return existing


def import_timeoff(entries: list, verbose=False):
    """Push time-off entries to Zendesk WFM. Returns (inserted_count, errors)."""
    if not entries:
        return 0, []

    resp = requests.post(
        f"{ZENDESK_WFM_BASE}/v1/timeOff/import",
        json={"data": entries},
        auth=zendesk_auth(),
        timeout=30,
    )
    if resp.status_code == 422:
        return 0, [f"422: {resp.text[:500]}"]
    resp.raise_for_status()
    result = resp.json()

    if not result.get("success"):
        return 0, [result.get("message", "Unknown error")]

    inserted = result.get("data", {}).get("inserted", [])
    return len(inserted), []


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def find_shifts_for_date(shifts: list, date_str: str) -> list[dict]:
    """Find all shifts that start on a specific date. Returns list of {startTime, endTime}."""
    target = datetime.strptime(date_str, "%Y-%m-%d").date()
    matched = []
    for shift in shifts:
        shift_date = datetime.fromtimestamp(shift["startTime"], tz=timezone.utc).date()
        if shift_date == target:
            matched.append(shift)
    return matched


def date_range(start_str: str, end_str: str):
    """Yield YYYY-MM-DD strings from start to end (inclusive)."""
    current = datetime.strptime(start_str, "%Y-%m-%d")
    end = datetime.strptime(end_str, "%Y-%m-%d")
    while current <= end:
        yield current.strftime("%Y-%m-%d")
        current += timedelta(days=1)


# ---------------------------------------------------------------------------
# Main sync logic
# ---------------------------------------------------------------------------
def resolve_from_wfm_map(email: str, name: str, wfm_map: dict, verbose=False):
    """Resolve a Slack email/name to a WFM agent ID using the WFM agent map.

    Tries: exact email match, local-part match across domains, firstname.lastname, then name match.
    """
    by_email = wfm_map["by_email"]
    by_name = wfm_map["by_name"]

    # Exact email match
    if email in by_email:
        return by_email[email]

    # Try matching by local part (e.g. kendall@getpoint.io → kendall@point.app)
    local_part = email.split("@")[0]
    for wfm_email, agent_id in by_email.items():
        if wfm_email.split("@")[0] == local_part:
            log(f"    Matched {email} → {wfm_email} (local part)", verbose_only=True, verbose=verbose)
            return agent_id

    # Try firstname.lastname variation
    name_parts = name.strip().split()
    if len(name_parts) >= 2:
        first_last = f"{name_parts[0].lower()}.{name_parts[-1].lower()}"
        for wfm_email, agent_id in by_email.items():
            if wfm_email.split("@")[0] == first_last:
                log(f"    Matched {email} → {wfm_email} (name-based)", verbose_only=True, verbose=verbose)
                return agent_id

    # Fallback: match by full name
    name_lower = name.strip().lower()
    if name_lower in by_name:
        log(f"    Matched {email} → name '{name}' in WFM", verbose_only=True, verbose=verbose)
        return by_name[name_lower]

    return None


def sync(dry_run=False, verbose=False, lookback_days=1):
    log(f"Syncing PTO from Slack → Zendesk WFM")

    # Step 1: Read PTO messages from Slack
    log("Step 1: Reading Slack channel...")
    messages = fetch_slack_messages(lookback_days=lookback_days, verbose=verbose)

    pto_requests = []
    for msg in messages:
        parsed = parse_pto_message(msg)
        if parsed:
            pto_requests.append(parsed)

    if not pto_requests:
        log("No PTO messages found in Slack. Nothing to sync.")
        return 0

    log(f"  Parsed {len(pto_requests)} PTO request(s)")
    for req in pto_requests:
        leave_type = "Sick" if req["reason_id"] == SICK_REASON_ID else "PTO"
        log(f"    {req['name']} ({req['email']}): {req['start_date']} → {req['end_date']} [{leave_type}]", verbose_only=True, verbose=verbose)

    # Step 2: Determine the full date range we need shifts for
    all_dates = []
    for req in pto_requests:
        all_dates.extend(list(date_range(req["start_date"], req["end_date"])))

    if not all_dates:
        log("No valid dates found. Nothing to sync.")
        return 0

    min_date = min(all_dates)
    max_date = max(all_dates)

    # Step 3: Fetch Zendesk WFM shifts
    log("Step 2: Fetching Zendesk WFM shifts...")
    shifts_by_agent = fetch_zendesk_shifts(min_date, max_date, verbose=verbose)

    # Step 4: Fetch existing Zendesk time-off
    log("Step 3: Checking existing Zendesk time-off...")
    existing_timeoff = fetch_existing_timeoff(min_date, max_date, verbose=verbose)

    # Step 5: Build WFM agent map (email/name → WFM agent ID)
    log("Step 4: Building WFM agent map...")
    wfm_map = build_wfm_agent_map(shifts_by_agent, verbose=verbose)
    log(f"  {len(wfm_map['by_email'])} WFM agents mapped")

    # Step 6: Match employees & build import entries
    log("Step 5: Matching employees & building entries...")
    to_import = []
    # Track per-message results for Slack reactions
    msg_results: dict[str, str] = {}  # slack_ts → "success" | "no_agent" | "skipped"
    skipped_no_agent = 0
    skipped_duplicate = 0
    skipped_past = 0
    blocked_no_shift = 0
    now_ts = int(datetime.now(timezone.utc).timestamp())

    for req in pto_requests:
        agent_id = resolve_from_wfm_map(req["email"], req["name"], wfm_map, verbose=verbose)

        if not agent_id:
            log(f"  SKIP: {req['name']} ({req['email']}) — no WFM agent")
            skipped_no_agent += 1
            if req["slack_ts"]:
                msg_results[req["slack_ts"]] = "no_agent"
            continue

        reason_id = req["reason_id"]
        leave_label = "Sick" if reason_id == SICK_REASON_ID else "PTO"
        agent_shifts = shifts_by_agent.get(agent_id, [])
        queued_any = False

        for day_str in date_range(req["start_date"], req["end_date"]):
            matched_shifts = find_shifts_for_date(agent_shifts, day_str)

            if not matched_shifts:
                # No schedule published yet — create a full-day block so WFM
                # won't schedule this person when the schedule is generated.
                day_dt = datetime.strptime(day_str, "%Y-%m-%d").replace(tzinfo=LOCAL_TZ)
                block_start = int(day_dt.timestamp())
                block_end = int((day_dt + timedelta(days=1)).timestamp())

                if block_end <= now_ts:
                    skipped_past += 1
                    continue

                if (agent_id, block_start, block_end) in existing_timeoff:
                    log(f"  SKIP: Duplicate block for {req['name']} on {day_str}", verbose_only=True, verbose=verbose)
                    skipped_duplicate += 1
                    continue

                entry = {
                    "agentId": agent_id,
                    "startTime": block_start,
                    "endTime": block_end,
                    "reasonId": reason_id,
                    "status": "approved",
                    "timeOffType": "full-day",
                    "note": f"Synced from Slack ({leave_label} block for {req['name']} — no schedule yet)",
                }
                to_import.append(entry)
                blocked_no_shift += 1
                queued_any = True
                log(f"  BLOCK: {req['name']} on {day_str} (full day, no schedule yet)", verbose_only=True, verbose=verbose)
                continue

            for shift in matched_shifts:
                start_time = shift["startTime"]
                end_time = shift["endTime"]

                if end_time <= now_ts:
                    log(f"  SKIP: Shift already ended for {req['name']} on {day_str}", verbose_only=True, verbose=verbose)
                    skipped_past += 1
                    continue

                if (agent_id, start_time, end_time) in existing_timeoff:
                    log(f"  SKIP: Duplicate for {req['name']} on {day_str}", verbose_only=True, verbose=verbose)
                    skipped_duplicate += 1
                    continue

                entry = {
                    "agentId": agent_id,
                    "startTime": start_time,
                    "endTime": end_time,
                    "reasonId": reason_id,
                    "status": "approved",
                    "timeOffType": "full-day",
                    "note": f"Synced from Slack ({leave_label} for {req['name']})",
                }
                to_import.append(entry)
                queued_any = True
                log(f"  QUEUE: {req['name']} on {day_str} (shift {start_time}→{end_time})", verbose_only=True, verbose=verbose)

        if req["slack_ts"]:
            msg_results[req["slack_ts"]] = "success" if queued_any else "skipped"

    # Summary
    log(f"\nSummary:")
    log(f"  To import:  {len(to_import)}")
    log(f"  Blocked (no schedule yet):  {blocked_no_shift}")
    log(f"  Skipped (no WFM agent):     {skipped_no_agent}")
    log(f"  Skipped (already ended):    {skipped_past}")
    log(f"  Skipped (duplicate):        {skipped_duplicate}")

    if dry_run:
        log("\n[DRY RUN] No changes made.")
        for entry in to_import:
            dt = datetime.fromtimestamp(entry["startTime"], tz=timezone.utc)
            log(f"  Would import: agent={entry['agentId']} date={dt.date()}")
        return 0

    if not to_import:
        log("Nothing to import.")
        _send_slack_reactions(msg_results, verbose=verbose)
        return 0

    # Step 7: Import to Zendesk WFM (batches of 25)
    log(f"\nImporting {len(to_import)} time-off entry/entries to Zendesk WFM...")
    total_inserted = 0
    batch_size = 25
    had_errors = False

    for i in range(0, len(to_import), batch_size):
        batch = to_import[i : i + batch_size]
        inserted, errors = import_timeoff(batch, verbose=verbose)
        if errors:
            log(f"  ERRORS in batch {i // batch_size + 1}: {errors}")
            had_errors = True
        else:
            total_inserted += inserted
            log(f"  Batch {i // batch_size + 1}: {inserted} imported")

    log(f"\nDone! {total_inserted} entries imported.")

    # Step 8: React to Slack messages
    _send_slack_reactions(msg_results, verbose=verbose)

    return 1 if had_errors else 0


def _send_slack_reactions(msg_results: dict[str, str], verbose=False):
    """Add Slack reactions based on per-message sync results."""
    if not msg_results:
        return

    log("Adding Slack reactions...")
    for ts, result in msg_results.items():
        if result == "success":
            slack_react(SLACK_CHANNEL_ID, ts, "white_check_mark", verbose=verbose)
        elif result == "no_agent":
            slack_react(SLACK_CHANNEL_ID, ts, "warning", verbose=verbose)
        # "skipped" = all duplicates/past — already processed, react with check
        elif result == "skipped":
            slack_react(SLACK_CHANNEL_ID, ts, "white_check_mark", verbose=verbose)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Sync PTO from Slack channel to Zendesk WFM"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would be synced without making changes",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Verbose output",
    )
    parser.add_argument(
        "--lookback-days", type=int, default=1,
        help="How many days back to read Slack messages (default: 1)",
    )
    args = parser.parse_args()

    # Validate config
    missing = []
    if not SLACK_BOT_TOKEN:
        missing.append("SLACK_BOT_TOKEN")
    if not ZENDESK_API_TOKEN:
        missing.append("ZENDESK_API_TOKEN")
    if not ZENDESK_EMAIL:
        missing.append("ZENDESK_EMAIL")

    if missing:
        print(f"ERROR: Missing required env vars: {', '.join(missing)}", file=sys.stderr)
        print("Add them to your .env file.", file=sys.stderr)
        sys.exit(1)

    try:
        exit_code = sync(
            dry_run=args.dry_run,
            verbose=args.verbose,
            lookback_days=args.lookback_days,
        )
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {e}", file=sys.stderr)
        if e.response is not None:
            print(f"Response: {e.response.text[:500]}", file=sys.stderr)
        sys.exit(1)
    except requests.exceptions.ConnectionError as e:
        print(f"Connection Error: {e}", file=sys.stderr)
        sys.exit(1)
    except RuntimeError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
