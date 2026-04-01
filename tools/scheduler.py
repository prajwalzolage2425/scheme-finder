"""
tools/scheduler.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Runs the scheme fetcher automatically on a schedule.
Runs once a week by default.

Usage:
    python tools/scheduler.py          # runs in background
    python tools/scheduler.py --now    # run immediately once

Keep this running on your server to keep schemes updated.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import time
import argparse
from datetime import datetime
from fetch_schemes import run as fetch_run

# ── Fetch Jobs ────────────────────────────────────────────────────────────────
# Each job runs in order with a 2-second delay between them
FETCH_JOBS = [
    {"query": "",           "state": "",            "size": 100, "label": "All schemes (top 100)"},
    {"query": "women",      "state": "",            "size": 50,  "label": "Women schemes"},
    {"query": "farmer",     "state": "",            "size": 50,  "label": "Agriculture schemes"},
    {"query": "scholarship","state": "",            "size": 50,  "label": "Education schemes"},
    {"query": "health",     "state": "",            "size": 50,  "label": "Health schemes"},
    {"query": "",           "state": "maharashtra", "size": 50,  "label": "Maharashtra state schemes"},
]

INTERVAL_HOURS = 168  # 7 days


def run_all_jobs():
    print(f"\n{'='*55}")
    print(f"  Auto-Fetch started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*55}")

    for i, job in enumerate(FETCH_JOBS, 1):
        print(f"\n[{i}/{len(FETCH_JOBS)}] {job['label']}")
        try:
            fetch_run(
                query        = job["query"],
                state_filter = job["state"],
                size         = job["size"],
                preview      = False,
                replace      = False  # always merge, never replace
            )
        except Exception as e:
            print(f"  ⚠️  Job failed: {e}")
        time.sleep(2)  # small delay between jobs

    print(f"\n✅ All jobs complete. Next run in {INTERVAL_HOURS} hours.")


def scheduler_loop():
    print(f"Scheduler started. Running every {INTERVAL_HOURS} hours.")
    while True:
        run_all_jobs()
        print(f"\n⏳ Sleeping {INTERVAL_HOURS} hours until next run...")
        time.sleep(INTERVAL_HOURS * 3600)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--now", action="store_true", help="Run immediately once and exit")
    args = parser.parse_args()

    if args.now:
        run_all_jobs()
    else:
        scheduler_loop()
