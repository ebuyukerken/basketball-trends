# File: scripts/extract_nba_schedule.py

import pandas as pd
import requests
import sys
import time
import pytz
from datetime import datetime, timezone
from pathlib import Path
import argparse

# --- Path Setup ---
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.append(str(SCRIPT_DIR))

try:
    from common.bigquery_loader import load_to_bigquery
except ImportError:
    print(f"Error: Could not import 'load_to_bigquery'.", file=sys.stderr)
    sys.exit(1)


def get_nba_static_schedule(target_date=None, target_season=None):
    """
    Fetches the NBA schedule from the static CDN endpoint.
    Returns a DataFrame with 'utc_start_time' and 'game_date' as datetime objects.
    """
    url = "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2.json"

    print(f"Fetching schedule data from NBA CDN...")

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"Error fetching data from NBA CDN: {e}", file=sys.stderr)
        return pd.DataFrame()

    league_schedule = data.get('leagueSchedule', {}).get('gameDates', [])
    processed_games = []

    eastern_tz = pytz.timezone('US/Eastern')
    utc_tz = pytz.utc

    print(f"Parsing schedule (Target Date: {target_date if target_date else 'Season ' + str(target_season)})...")

    for day_data in league_schedule:
        # 'gameDate' field in JSON can vary (e.g., "01/20/2026 00:00:00" or "2026-01-20 00:00:00")
        raw_date = day_data['gameDate']

        # --- Date Normalization Logic ---
        try:
            # Extract the date part (ignore time)
            date_part = raw_date.split(' ')[0]

            # Normalize to YYYY-MM-DD
            if '/' in date_part:
                # Format: MM/DD/YYYY (Common in NBA CDN)
                dt_obj = datetime.strptime(date_part, '%m/%d/%Y')
            else:
                # Format: YYYY-MM-DD (ISO format)
                dt_obj = datetime.strptime(date_part, '%Y-%m-%d')

            current_date_str = dt_obj.strftime('%Y-%m-%d')

        except ValueError as e:
            print(f"Warning: Could not parse date '{raw_date}'. Error: {e}")
            continue
        # --------------------------------

        # FILTER: Strict date filtering
        # Now we compare normalized 'current_date_str' with the 'target_date' input
        if target_date and current_date_str != target_date:
            continue

        for game in day_data.get('games', []):
            try:
                utc_start_time_str = None

                # Option A: Check if UTC is explicitly provided
                if 'gameDateTimeUTC' in game:
                    utc_start_time_str = game['gameDateTimeUTC']

                # Option B: Convert from Eastern Time
                elif 'gameDateTimeEst' in game:
                    est_str = game['gameDateTimeEst']
                    dt_str_clean = est_str.replace('Z', '')
                    dt_obj = datetime.fromisoformat(dt_str_clean)

                    dt_eastern = eastern_tz.localize(dt_obj)
                    dt_utc = dt_eastern.astimezone(utc_tz)
                    # Keep as string for now, convert to object in DataFrame later
                    utc_start_time_str = dt_utc.isoformat()

                game_record = {
                    "game_id": game['gameId'],
                    "season_id": game.get('seasonId', ''),
                    "game_date": current_date_str,  # Standardized String: "YYYY-MM-DD"
                    "home_team": game['homeTeam']['teamName'],
                    "home_team_id": game['homeTeam']['teamId'],
                    "visitor_team": game['awayTeam']['teamName'],
                    "visitor_team_id": game['awayTeam']['teamId'],
                    "game_status": game.get('gameStatusText', ''),
                    "utc_start_time": utc_start_time_str,  # String: ISO Format
                    "arena_name": game.get('arenaName', ''),
                    "city": game.get('arenaCity', '')
                }

                processed_games.append(game_record)

            except Exception as inner_e:
                print(f"Warning: Failed to process game {game.get('gameId')}: {inner_e}")
                continue

    df = pd.DataFrame(processed_games)

    # --- TYPE CONVERSION ---
    if not df.empty:
        # Convert 'utc_start_time' string to datetime objects (UTC aware)
        df['utc_start_time'] = pd.to_datetime(df['utc_start_time'], utc=True, errors='coerce')

        # Convert 'game_date' string to datetime objects
        df['game_date'] = pd.to_datetime(df['game_date'], errors='coerce')

    print(f"Processed {len(df)} games.")

    return df


def run_schedule_extraction(date_str=None, season_str=None):
    """
    Main extraction controller.
    Strictly requires either date_str or season_str.
    """
    start_time = time.time()
    bq_table_name = "raw.nba_schedule"

    if season_str:
        print(f"--- Starting NBA Schedule Backfill for Season: {season_str} ---")
        schedule_df = get_nba_static_schedule(target_season=season_str)
        load_mode = "overwrite"

    elif date_str:
        print(f"--- Starting NBA Schedule Extraction for Date: {date_str} ---")
        schedule_df = get_nba_static_schedule(target_date=date_str)
        # Daily runs append/upsert logic
        load_mode = "append"

    else:
        # This should technically be unreachable due to argparse required=True
        raise ValueError("Error: You must provide either --date or --season.")

    # Add Load Timestamp
    if not schedule_df.empty:
        load_timestamp_utc = datetime.now(timezone.utc)
        schedule_df["_loaded_at_utc"] = load_timestamp_utc

        print(f"Loading {len(schedule_df)} schedule records to '{bq_table_name}' (Mode: {load_mode})")

        load_to_bigquery(
            schedule_df,
            bq_table_name,
            write_mode=load_mode
        )
    else:
        print(f"No games found for the specified criteria ({date_str if date_str else season_str}). Skipping load.")

    end_time = time.time()
    print(f"\n--- NBA Schedule Extraction Finished in {end_time - start_time:.2f} seconds ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract NBA Schedule/Fixtures.")

    # Mutually exclusive AND required group
    group = parser.add_mutually_exclusive_group(required=True)

    group.add_argument(
        "--date",
        type=str,
        help="Specific date to extract in YYYY-MM-DD format (Airflow run_ds)."
    )
    group.add_argument(
        "--season",
        type=str,
        help="Season string (e.g. '2025-26') to overwrite full schedule."
    )

    args = parser.parse_args()

    run_schedule_extraction(date_str=args.date, season_str=args.season)

    #python scripts/extract_nba_schedule.py --season "2025-26"