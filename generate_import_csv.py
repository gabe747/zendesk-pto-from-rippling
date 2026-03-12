#!/usr/bin/env python3
"""Generate Zendesk WFM schedule import CSV from Excel spreadsheet for March 2026.

Reads the Member Services Schedule Excel file, parses the March 2026 sheet,
and outputs a CSV in the Zendesk WFM import format.

Handles:
- Regular shifts (e.g., "7AM - 3PM") as workstream entries
- Overnight shifts (e.g., "10PM - 8AM", "4PM - 12AM") correctly spanning days
- PTO / Full PTO as full-day Paid Leave time-off
- Sick as full-day Sick Leave time-off
- Half Day PTO as partial Paid Leave time-off
- OFF and Training are skipped (no CSV entry)
"""

import csv
import re
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

ET = ZoneInfo('America/New_York')

# ── Zendesk WFM IDs ──────────────────────────────────────────────────────────
TICKETS_WORKSTREAM_ID = 'b9a6c218-b0fb-417d-82bf-b36cb3b8f332'
PAID_LEAVE_REASON_ID = '43d7520d-3fe3-4a58-ae8e-0b299c796700'
SICK_LEAVE_REASON_ID = 'd9943065-b864-429e-b6df-2e03641ed846'

# ── Agent mapping: spreadsheet first name → (Zendesk name, Zendesk email) ────
AGENT_MAP = {
    'AJ':        ('AJ Haynes',            'aj@getpoint.io'),
    'Amanda':    ('Amanda Haley',          'amanda@getpoint.io'),
    'Anna':      ('Anna Long',             'anna@getpoint.io'),
    'Britt':     ('Britt Grass',           'britt@getpoint.io'),
    'Caleb':     ('Caleb McClure',         'caleb@getpoint.io'),
    'Caroline':  ('Caroline Manney',       'carolinemanney@getpoint.io'),
    'Cassidy':   ('Cassidy Kling',         'cassidy@point.app'),
    'Colton':    ('Colton Haney',          'colton@getpoint.io'),
    'Crystal':   ('Crystal Villalobos',    'crystal@getpoint.io'),
    'Entrisse':  ('Entrisse Mackson',      'entrisse@getpoint.io'),
    'Jake':      ('Jake Fishman',          'jake@getpoint.io'),
    'Jamillah':  ('Jamillah Hendricks',    'jamillah@atlascard.com'),
    'Jessica':   ('Jess Pfotenhauer',      'jessica@atlascard.com'),
    'Joelanar':  ('Joelanar Byam',         'joelanar@atlascard.com'),
    'Kaitlin':   ('Kaitlin Kahrs',         'kaitlin@getpoint.io'),
    'Kendall':   ('Kendall Barker',        'kendall@point.app'),
    'Mae':       ('Mae Johnson',           'mae@getpoint.io'),
    'Nicky':     ('Nicky Park',            'nicky@getpoint.io'),
    'Owen':      ('Owen Heckman',          'owen@getpoint.io'),
    'Sam':       ('Sam Crawley',           'samantha@getpoint.io'),
    'Sara':      ('Sara Masiello',         'sara@getpoint.io'),
    'Simon':     ('Simon Duyungan',        'simon@getpoint.io'),
    'Summer':    ('Summer Bontrager',      'summer.bontrager@atlascard.com'),
}

EXCEL_PATH = '/Users/gabewhitlatch/Downloads/Member Services Schedule.xlsx'
OUTPUT_CSV = '/Users/gabewhitlatch/Downloads/zendesk_schedule_march_2026.csv'


# ── Time parsing ─────────────────────────────────────────────────────────────
def parse_hour(t: str) -> int | None:
    """Parse '7AM', '10PM', '12AM', '12PM' → hour 0–23."""
    t = t.strip().upper()
    m = re.match(r'^(\d{1,2})\s*(AM|PM)$', t)
    if not m:
        return None
    h, ampm = int(m.group(1)), m.group(2)
    if ampm == 'AM':
        return 0 if h == 12 else h
    else:
        return 12 if h == 12 else h + 12


def parse_shift(val: str) -> tuple[int, int] | None:
    """Parse '7AM - 3PM' → (start_hour, end_hour) or None."""
    val = val.strip().upper()
    for sep in (' - ', ' – ', '-', '–'):
        if sep in val:
            parts = val.split(sep, 1)
            if len(parts) == 2:
                sh = parse_hour(parts[0])
                eh = parse_hour(parts[1])
                if sh is not None and eh is not None:
                    return (sh, eh)
    return None


def make_iso(date_str: str, hour: int, minute: int = 0, second: int = 0,
             next_day: bool = False) -> str:
    """Create ISO 8601 timestamp in ET from date string + time components."""
    dt = datetime.strptime(date_str, '%Y-%m-%d')
    if next_day:
        dt += timedelta(days=1)
    dt = dt.replace(hour=hour, minute=minute, second=second, tzinfo=ET)
    return dt.isoformat()


# ── Excel parsing ────────────────────────────────────────────────────────────
def parse_excel() -> dict[str, dict[str, str]]:
    """Parse March 2026 sheet → {first_name: {date_str: cell_value}}."""
    df = pd.read_excel(EXCEL_PATH, sheet_name='March 2026', header=None)
    schedule: dict[str, dict[str, str]] = {}
    rows = df.values.tolist()
    i = 0

    while i < len(rows):
        row = rows[i]
        if str(row[1]).strip() == 'Day of the Week' and i + 1 < len(rows):
            date_row = rows[i + 1]
            dates: list[str | None] = []
            for col in range(2, 9):  # Mon–Sun columns
                val = date_row[col]
                if pd.notna(val):
                    if isinstance(val, datetime):
                        # Dates in the sheet say 2025 but should be 2026
                        d = val.replace(year=2026)
                        dates.append(d.strftime('%Y-%m-%d'))
                    else:
                        dates.append(str(val))
                else:
                    dates.append(None)

            j = i + 2
            while j < len(rows):
                prow = rows[j]
                name = str(prow[1]).strip() if pd.notna(prow[1]) else ''
                if name in ('Day of the Week', 'Date'):
                    break  # Hit next week's header
                if name in ('', 'nan'):
                    j += 1
                    continue  # Skip empty separator rows between pods
                if name not in schedule:
                    schedule[name] = {}
                for col_idx, date_str in enumerate(dates):
                    if date_str is None:
                        continue
                    cell = prow[col_idx + 2]
                    if pd.notna(cell):
                        schedule[name][date_str] = str(cell).strip()
                j += 1
            i = j
        else:
            i += 1

    return schedule


# ── CSV generation ───────────────────────────────────────────────────────────
def generate():
    print("Parsing Excel schedule...")
    schedule = parse_excel()
    print(f"  Found {len(schedule)} people in spreadsheet\n")

    csv_rows: list[dict] = []
    warnings: list[str] = []
    stats = {'shifts': 0, 'pto': 0, 'sick': 0, 'half_pto': 0,
             'off': 0, 'training': 0, 'unparsed': 0}

    for xl_name in sorted(schedule.keys()):
        agent = AGENT_MAP.get(xl_name)
        if not agent:
            warnings.append(f"⚠  '{xl_name}' not found in agent map — skipped entirely")
            continue

        full_name, email = agent
        person_shifts = 0

        for date_str in sorted(schedule[xl_name].keys()):
            val = schedule[xl_name][date_str]
            val_upper = val.upper().strip()

            # ── OFF / empty ──
            if val_upper in ('OFF', 'NAN', ''):
                stats['off'] += 1
                continue

            # ── Training ──
            if val_upper == 'TRAINING':
                stats['training'] += 1
                continue

            # ── Full PTO ──
            if val_upper in ('PTO', 'FULL PTO'):
                csv_rows.append({
                    'agentName': full_name,
                    'agentEmail': email,
                    'taskType': 'timeOff',
                    'taskId': PAID_LEAVE_REASON_ID,
                    'taskName': 'Paid Leave',
                    'timeOffType': 'full-day',
                    'startTime': make_iso(date_str, 0, 0, 0),
                    'endTime': make_iso(date_str, 23, 59, 0),
                })
                stats['pto'] += 1
                continue

            # ── Half Day PTO ──
            if val_upper == 'HALF DAY PTO':
                csv_rows.append({
                    'agentName': full_name,
                    'agentEmail': email,
                    'taskType': 'timeOff',
                    'taskId': PAID_LEAVE_REASON_ID,
                    'taskName': 'Paid Leave',
                    'timeOffType': 'partial',
                    'startTime': make_iso(date_str, 0, 0, 0),
                    'endTime': make_iso(date_str, 23, 59, 0),
                })
                stats['half_pto'] += 1
                warnings.append(
                    f"  ℹ  {xl_name} on {date_str}: 'Half Day PTO' — added as partial time-off (may need manual adjustment)")
                continue

            # ── Sick ──
            if val_upper == 'SICK':
                csv_rows.append({
                    'agentName': full_name,
                    'agentEmail': email,
                    'taskType': 'timeOff',
                    'taskId': SICK_LEAVE_REASON_ID,
                    'taskName': 'Sick Leave',
                    'timeOffType': 'full-day',
                    'startTime': make_iso(date_str, 0, 0, 0),
                    'endTime': make_iso(date_str, 23, 59, 0),
                })
                stats['sick'] += 1
                continue

            # ── Regular shift (e.g., "7AM - 3PM") ──
            shift = parse_shift(val)
            if shift is None:
                stats['unparsed'] += 1
                warnings.append(f"  ⚠  Could not parse '{val}' for {xl_name} on {date_str}")
                continue

            start_hour, end_hour = shift

            # Overnight: end time <= start time means it wraps to the next day
            # e.g. 10PM-8AM → 22:00 today to 08:00 tomorrow
            # e.g. 4PM-12AM → 16:00 today to 00:00 tomorrow
            next_day = end_hour <= start_hour

            csv_rows.append({
                'agentName': full_name,
                'agentEmail': email,
                'taskType': 'workstream',
                'taskId': TICKETS_WORKSTREAM_ID,
                'taskName': 'Tickets',
                'timeOffType': '',
                'startTime': make_iso(date_str, start_hour),
                'endTime': make_iso(date_str, end_hour, next_day=next_day),
            })
            stats['shifts'] += 1
            person_shifts += 1

    # ── Write CSV ────────────────────────────────────────────────────────────
    fieldnames = ['agentName', 'agentEmail', 'taskType', 'taskId',
                  'taskName', 'timeOffType', 'startTime', 'endTime']

    with open(OUTPUT_CSV, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in csv_rows:
            writer.writerow(row)

    # ── Summary ──────────────────────────────────────────────────────────────
    print(f"{'='*60}")
    print(f"  CSV generated: {OUTPUT_CSV}")
    print(f"{'='*60}")
    print(f"  Total rows:       {len(csv_rows)}")
    print(f"    Shifts:         {stats['shifts']}")
    print(f"    PTO (full):     {stats['pto']}")
    print(f"    PTO (half):     {stats['half_pto']}")
    print(f"    Sick:           {stats['sick']}")
    print(f"  Skipped:")
    print(f"    OFF days:       {stats['off']}")
    print(f"    Training:       {stats['training']}")
    if stats['unparsed']:
        print(f"    Unparsed:       {stats['unparsed']}")

    # Per-agent summary
    agent_counts: dict[str, int] = {}
    for row in csv_rows:
        n = row['agentName']
        agent_counts[n] = agent_counts.get(n, 0) + 1
    print(f"\n  Per-agent entry counts:")
    for name in sorted(agent_counts):
        print(f"    {name:25s}  {agent_counts[name]:3d}")

    if warnings:
        print(f"\n{'─'*60}")
        print(f"  Notes & warnings ({len(warnings)}):")
        for w in warnings:
            print(f"    {w}")

    print(f"\nDone! Import this CSV via Zendesk WFM: Schedule → CSV icon → Import schedule")


if __name__ == '__main__':
    generate()
