# File: scripts/extract_nba_schedule.py

import pandas as pd
import pytz
from nba_api.stats.endpoints import scoreboardv2
from datetime import datetime, timedelta, timezone
import sys
from pathlib import Path

# --- Path Setup ---
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.append(str(SCRIPT_DIR))

try:
    from common.bigquery_loader import load_to_bigquery
except ImportError as e:
    print(f"Error: Could not import 'load_to_bigquery'", file=sys.stderr)
    sys.exit(1)

# --- Timezone Configuration ---
# NBA game times are provided in Eastern Time
ET_TZ = pytz.timezone('America/New_York')


def parse_game_time_to_utc(game_date_str: str, game_time_str: str) -> datetime:
    """
    Parses the NBA's ET game time string and a game date
    into a proper UTC datetime object.

    Args:
        game_date_str (str): The game date, e.g., '2025-11-09'
        game_time_str (str): The game time from the API, e.g., '7:00 PM ET'
    """
    try:
        # 1. Create a "naive" datetime object from the time string
        # e.g., '7:00 PM' -> 19:00:00
        time_obj = datetime.strptime(game_time_str.replace(' ET', ''), '%I:%M %p').time()

        # 2. Get the date object
        date_obj = datetime.strptime(game_date_str, '%Y-%m-%d').date()

        # 3. Combine them into a single "naive" datetime
        naive_dt = datetime.combine(date_obj, time_obj)

        # 4. Localize the "naive" datetime to Eastern Time
        et_dt = ET_TZ.localize(naive_dt)

        # 5. Convert (normalize) the ET datetime to UTC
        utc_dt = et_dt.astimezone(pytz.utc)

        return utc_dt
    except Exception as e:
        print(f"Warning: Could not parse time '{game_time_str}'. Error: {e}", file=sys.stderr)
        return None


def run_schedule_extraction():
    """Main ETL function"""
    print("--- Starting NBA Schedule Extraction ---")
    start_time = pd.Timestamp.now()

    # 1. Get yesterday's date
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    date_str_api = yesterday.strftime("%m/%d/%Y")
    date_str_iso = yesterday.strftime("%Y-%m-%d")

    # 2. Fetch schedule
    try:
        print(f"Fetching schedule for: {date_str_iso}...")

        # A) Call the API
        scoreboard = scoreboardv2.ScoreboardV2(game_date=date_str_api)
        game_headers_df = scoreboard.game_header.get_data_frame()

        if game_headers_df.empty:
            print("No games found for this date. Exiting.")
            return

        print(f"Found {len(game_headers_df)} games.")

        # B) We only need a few columns
        schedule_df = game_headers_df[['GAME_ID', 'GAME_STATUS_TEXT']].copy()

        # C) Convert the times to UTC
        print("Converting game times to UTC...")
        schedule_df['game_start_utc'] = schedule_df['GAME_STATUS_TEXT'].apply(
            lambda time_str: parse_game_time_to_utc(date_str_iso, time_str)
        )

        # D) Rename for clarity
        schedule_df = schedule_df.rename(columns={'GAME_STATUS_TEXT': 'game_time_et_str'})

        final_df = schedule_df[['GAME_ID', 'game_start_utc', 'game_time_et_str']]

        # 3. Add load timestamp and load to BQ
        final_df["_loaded_at_utc"] = datetime.now(timezone.utc)

        # Remove any rows where UTC conversion failed
        # (e.g., if a game time was "Final" instead of "7:00 PM ET")
        final_df = final_df.dropna(subset=['game_start_utc'])

        if not final_df.empty:
            load_to_bigquery(
                final_df,
                "raw.nba_schedule",
                write_mode="append"  # We append daily
            )
        else:
            print("No schedule data with valid start times to load.")

    except Exception as e:
        print(f"Error in schedule extraction: {e}", file=sys.stderr)

    end_time = pd.Timestamp.now()
    print(f"\n--- NBA Schedule Extraction Finished in {(end_time - start_time).total_seconds():.2f} seconds ---")


if __name__ == "__main__":
    run_schedule_extraction()