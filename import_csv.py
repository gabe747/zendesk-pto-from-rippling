#!/usr/bin/env python3
"""One-off script to import PTO from a CSV file into Zendesk WFM."""

import csv
import sys
from datetime import datetime, timedelta, timezone

# Alternate email domains to try if the CSV email doesn't resolve in Zendesk
ALTERNATE_DOMAINS = ["atlascard.com", "point.app"]

from sync_pto import (
    LOCAL_TZ,
    DEFAULT_REASON_ID,
    build_wfm_agent_map,
    date_range,
    fetch_existing_timeoff,
    fetch_zendesk_shifts,
    find_shifts_for_date,
    import_timeoff,
    log,
    zendesk_auth,
)


def resolve_from_wfm_map(email: str, name: str, wfm_map: dict, verbose=False):
    """Resolve a CSV email/name to a WFM agent ID using the WFM agent map.

    Tries: exact email match, local-part match across domains, then name match.
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


def parse_csv(path: str) -> list[dict]:
    """Parse the PTO CSV into a list of request dicts."""
    requests = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row["Employee"].strip()
            email = row["Work email"].strip().lower()
            start_str = row["Leave Start Date"].strip()
            end_str = row["Leave End Date"].strip()

            start_date = datetime.strptime(start_str, "%m/%d/%y").strftime("%Y-%m-%d")
            end_date = datetime.strptime(end_str, "%m/%d/%y").strftime("%Y-%m-%d")

            requests.append({
                "name": name,
                "email": email,
                "start_date": start_date,
                "end_date": end_date,
            })
    return requests


def main():
    dry_run = "--dry-run" in sys.argv
    verbose = "--verbose" in sys.argv or "-v" in sys.argv
    csv_path = "/Users/gabewhitlatch/Downloads/PTO Calendar.csv"

    log(f"Importing PTO from CSV → Zendesk WFM")

    # Step 1: Parse CSV
    log("Step 1: Parsing CSV...")
    pto_requests = parse_csv(csv_path)
    log(f"  Found {len(pto_requests)} PTO entries")

    # Step 2: Determine full date range
    all_dates = []
    for req in pto_requests:
        all_dates.extend(list(date_range(req["start_date"], req["end_date"])))

    min_date = min(all_dates)
    max_date = max(all_dates)
    log(f"  Date range: {min_date} → {max_date}")

    # Step 3: Fetch shifts and existing time-off
    log("Step 2: Fetching Zendesk WFM shifts...")
    shifts_by_agent = fetch_zendesk_shifts(min_date, max_date, verbose=verbose)

    log("Step 3: Checking existing Zendesk time-off...")
    existing_timeoff = fetch_existing_timeoff(min_date, max_date, verbose=verbose)

    # Step 4: Build WFM agent map (email/name → WFM agent ID)
    log("Step 4: Building WFM agent map...")
    wfm_map = build_wfm_agent_map(shifts_by_agent, verbose=verbose)
    log(f"  {len(wfm_map['by_email'])} WFM agents mapped")

    # Step 5: Match employees and build entries
    log("Step 5: Matching employees & building entries...")
    to_import = []
    skipped_no_agent = 0
    skipped_duplicate = 0
    skipped_past = 0
    blocked_no_shift = 0
    now_ts = int(datetime.now(timezone.utc).timestamp())
    no_agent_emails = set()

    for req in pto_requests:
        agent_id = resolve_from_wfm_map(req["email"], req["name"], wfm_map, verbose=verbose)

        if not agent_id:
            if req["email"] not in no_agent_emails:
                log(f"  SKIP: {req['name']} ({req['email']}) — no Zendesk account")
                no_agent_emails.add(req["email"])
            skipped_no_agent += 1
            continue

        agent_shifts = shifts_by_agent.get(agent_id, [])

        for day_str in date_range(req["start_date"], req["end_date"]):
            matched_shifts = find_shifts_for_date(agent_shifts, day_str)

            if not matched_shifts:
                day_dt = datetime.strptime(day_str, "%Y-%m-%d").replace(tzinfo=LOCAL_TZ)
                block_start = int(day_dt.timestamp())
                block_end = int((day_dt + timedelta(days=1)).timestamp())

                if block_end <= now_ts:
                    skipped_past += 1
                    continue

                if (agent_id, block_start, block_end) in existing_timeoff:
                    skipped_duplicate += 1
                    continue

                entry = {
                    "agentId": agent_id,
                    "startTime": block_start,
                    "endTime": block_end,
                    "reasonId": DEFAULT_REASON_ID,
                    "status": "approved",
                    "timeOffType": "full-day",
                    "note": f"CSV import (PTO block for {req['name']} — no schedule yet)",
                }
                to_import.append(entry)
                blocked_no_shift += 1
                log(f"  BLOCK: {req['name']} on {day_str}", verbose_only=True, verbose=verbose)
                continue

            for shift in matched_shifts:
                start_time = shift["startTime"]
                end_time = shift["endTime"]

                if end_time <= now_ts:
                    skipped_past += 1
                    continue

                if (agent_id, start_time, end_time) in existing_timeoff:
                    skipped_duplicate += 1
                    continue

                entry = {
                    "agentId": agent_id,
                    "startTime": start_time,
                    "endTime": end_time,
                    "reasonId": DEFAULT_REASON_ID,
                    "status": "approved",
                    "timeOffType": "full-day",
                    "note": f"CSV import (PTO for {req['name']})",
                }
                to_import.append(entry)
                log(f"  QUEUE: {req['name']} on {day_str}", verbose_only=True, verbose=verbose)

    # Deduplicate entries (same agent, same start/end)
    seen = set()
    deduped = []
    for entry in to_import:
        key = (entry["agentId"], entry["startTime"], entry["endTime"])
        if key not in seen:
            seen.add(key)
            deduped.append(entry)
    dup_removed = len(to_import) - len(deduped)
    to_import = deduped

    # Summary
    log(f"\nSummary:")
    log(f"  To import:              {len(to_import)}")
    log(f"  Blocked (no schedule):  {blocked_no_shift}")
    log(f"  Skipped (no ZD agent):  {skipped_no_agent} ({len(no_agent_emails)} unique emails)")
    log(f"  Skipped (already ended):{skipped_past}")
    log(f"  Skipped (duplicate):    {skipped_duplicate}")
    if dup_removed:
        log(f"  Deduped (CSV overlap):  {dup_removed}")

    if dry_run:
        log("\n[DRY RUN] No changes made.")
        return

    if not to_import:
        log("Nothing to import.")
        return

    # Import in batches of 25 (WFM API limit)
    log(f"\nImporting {len(to_import)} entries to Zendesk WFM...")
    total_inserted = 0
    batch_size = 25
    for i in range(0, len(to_import), batch_size):
        batch = to_import[i : i + batch_size]
        inserted, errors = import_timeoff(batch, verbose=verbose)
        if errors:
            log(f"  ERRORS in batch {i // batch_size + 1}: {errors}")
        else:
            total_inserted += inserted
            log(f"  Batch {i // batch_size + 1}: {inserted} imported")

    log(f"\nDone! {total_inserted} entries imported.")


if __name__ == "__main__":
    main()
