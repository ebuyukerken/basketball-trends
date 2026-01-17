# File: scripts/extract_nba.py

import pandas as pd
from nba_api.stats.endpoints import playergamelogs, commonallplayers
from datetime import datetime, timedelta, timezone
import sys
import time
import json
from pathlib import Path
import argparse  # For handling command-line arguments

# --- Path Setup ---
# This ensures we can import fpython scripts\rom the 'common' folder
# whether we run this script locally or from the Airflow container.
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.append(str(SCRIPT_DIR))

try:
    # Import the reusable BigQuery loading function
    from common.bigquery_loader import load_to_bigquery
except ImportError as e:
    print(f"Error: Could not import 'load_to_bigquery' from 'common.bigquery_loader.py'", file=sys.stderr)
    print("Please ensure 'scripts/common/bigquery_loader.py' exists.", file=sys.stderr)
    # Also, create an empty 'scripts/common/__init__.py' file to make it a package
    sys.exit(1)


def get_season_str(date_obj):
    """
    Gets the NBA season string (e.g., '2025-26') from a date object.
    The NBA season flips in October.
    """
    if date_obj.month >= 10:
        year_start = date_obj.year
        year_end = (date_obj.year + 1) % 100  # Get last two digits
    else:
        year_start = date_obj.year - 1
        year_end = date_obj.year % 100  # Get last two digits

    return f"{year_start}-{year_end:02d}"

def fetch_player_gamelogs(date_from=None, date_to=None, season=None, timeout_val=60):
    """
    Fetches player game logs... (rest of docstring)
    """

    season_types_to_try = ["Regular Season", "Playoffs", "Pre Season"]
    all_logs_df = pd.DataFrame()

    if season:
        log_context = f"season {season}"
    else:
        log_context = f"dates {date_from} to {date_to}"

    print(f"Fetching game logs for {log_context}...")

    for season_type in season_types_to_try:
        print(f"  -> Trying season type: '{season_type}'...")
        try:
            logs = playergamelogs.PlayerGameLogs(
                season_nullable=season,
                season_type_nullable=season_type,
                date_from_nullable=date_from,
                date_to_nullable=date_to,
                timeout=timeout_val
            )
            # This is the line that can fail
            logs_df = logs.get_data_frames()[0]

            if not logs_df.empty:
                print(f"     Found {len(logs_df)} records.")
                all_logs_df = pd.concat([all_logs_df, logs_df], ignore_index=True)
            else:
                print(f"     No records found.")

        # Catch the specific error when the API returns empty/bad text
        except json.JSONDecodeError:
            print(f"     No data returned (JSONDecodeError). Likely no games for this type.")
            pass  # Just means no data, so we continue

        except Exception as e:
            # Catch any other unexpected errors
            print(f"     Error fetching '{season_type}': {e}", file=sys.stderr)
            pass

        # Add a small delay to be polite to the API
        time.sleep(2)

    print(f"\nSuccessfully fetched {len(all_logs_df)} total game log records across all types.")

    if not all_logs_df.empty:
        try:
            # The API returns GAME_DATE as a string like 'NOV 09, 2025'
            # or 'MM/DD/YYYY'. We convert it to a standard YYYY-MM-DD.

            # pd.to_datetime is smart and can handle multiple formats
            all_logs_df['GAME_DATE'] = pd.to_datetime(all_logs_df['GAME_DATE'])

            # Now we format it as a clean string for BigQuery.
            # This is better than letting BQ guess.
            all_logs_df['GAME_DATE'] = all_logs_df['GAME_DATE'].dt.strftime('%Y-%m-%d')

            print("Successfully cleaned and standardized 'GAME_DATE' column.")
        except Exception as e:
            print(f"Warning: Could not clean GAME_DATE column: {e}", file=sys.stderr)

    return all_logs_df


def fetch_all_players(timeout_val=60):
    """Fetches the complete list of all NBA players."""
    print("Fetching master player list...")
    try:
        # We set 'is_only_current_season=0' to get ALL players (historical)
        players = commonallplayers.CommonAllPlayers(
            is_only_current_season=0,
            timeout=timeout_val
        )
        players_df = players.get_data_frames()[0]
        print(f"Found {len(players_df)} total players in master list.")
        return players_df
    except Exception as e:
        print(f"Error fetching player list: {e}", file=sys.stderr)
        return pd.DataFrame()  # Return empty DF on failure


def run_nba_extraction(date_str=None, season_str=None):
    """
    Main extraction function.
    """
    start_time = time.time()

    # 1. Determine what game logs to fetch and the BigQuery load mode
    if season_str:
        print(f"--- Starting NBA Backfill for Season: {season_str} ---")
        gamelogs_df = fetch_player_gamelogs(season=season_str)
        gamelog_load_mode = "overwrite"

    elif date_str:
        print(f"--- Starting NBA Rerun for Date: {date_str} ---")

        # --- FIX START ---
        # Convert the date string to a datetime object to calculate the season
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        calculated_season = get_season_str(date_obj)

        # Pass the calculated season to the fetch function
        gamelogs_df = fetch_player_gamelogs(
            date_from=date_str,
            date_to=date_str,
            season=calculated_season
        )
        # --- FIX END ---

        gamelog_load_mode = "append"

    else:
        # Default "daily run" behavior
        yesterday = datetime.now() - timedelta(days=1)
        yesterday_str = yesterday.strftime("%Y-%m-%d")
        daily_season_str = get_season_str(yesterday)
        print(f"--- Starting NBA Daily Extraction for {yesterday_str} ---")
        gamelogs_df = fetch_player_gamelogs(date_from=yesterday_str, date_to=yesterday_str,
                                            season=daily_season_str)
        # For daily runs, we append
        gamelog_load_mode = "append"

    # 2. Fetch Player List (Always run this to keep it fresh)
    player_list_df = fetch_all_players()

    # 3. Add load timestamp (for deduplication in dbt)
    load_timestamp_utc = datetime.now(timezone.utc)

    if not gamelogs_df.empty:
        gamelogs_df["_loaded_at_utc"] = load_timestamp_utc

    if not player_list_df.empty:
        player_list_df["_loaded_at_utc"] = load_timestamp_utc

    # 4. Load Game Logs to BigQuery
    if not gamelogs_df.empty:
        print(f"Loading {len(gamelogs_df)} game logs to 'raw.nba_game_logs' (Mode: {gamelog_load_mode})")
        load_to_bigquery(
            gamelogs_df,
            "raw.nba_game_logs",
            write_mode=gamelog_load_mode
        )
    else:
        print("No game logs found to load.")

    # Load Player List to BigQuery (Always overwrite)
    if not player_list_df.empty:
        print(f"Loading {len(player_list_df)} players to 'raw.nba_player_list' (Mode: overwrite)")
        load_to_bigquery(
            player_list_df,
            "raw.nba_player_list",
            write_mode="overwrite"
        )
    else:
        print("Failed to fetch player list, skipping load.", file=sys.stderr)

    end_time = time.time()
    print(f"\n--- NBA Extraction Finished in {end_time - start_time:.2f} seconds ---")


if __name__ == "__main__":
    # --- Argument Parsing ---
    parser = argparse.ArgumentParser(description="Extract NBA data.")

    # Add mutually exclusive arguments. You can't run a backfill AND a daily at the same time.
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--date",
        type=str,
        help="A specific date to run for in YYYY-MM-DD format."
    )
    group.add_argument(
        "--season",
        type=str,
        help="A specific season to backfill, e.g., '2025-26'."
    )

    args = parser.parse_args()

    # Call the main function with the (optional) arguments
    run_nba_extraction(date_str=args.date, season_str=args.season)