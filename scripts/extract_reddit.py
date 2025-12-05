import os
import sys
import praw
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import time

# --- Path Setup ---
# Add 'scripts' directory to the path to find 'common'
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.append(str(SCRIPT_DIR))

try:
    # Import our BigQuery client and loader function
    from common.bigquery_loader import load_to_bigquery, BQ_CLIENT, PROJECT_ID
except ImportError as e:
    print("Error: Could not import from 'common.bigquery_loader'", file=sys.stderr)
    print(f"Details: {e}", file=sys.stderr)
    sys.exit(1)


# --- Load Environment & Reddit Client ---
def setup_reddit_client():
    """
    Loads secrets from .env and initializes the PRAW client.
    """
    # Load .env file from project root (2 levels up)
    PROJECT_ROOT = SCRIPT_DIR.parent
    load_dotenv(PROJECT_ROOT / ".env")

    try:
        reddit_client = praw.Reddit(
            client_id=os.getenv("REDDIT_CLIENT_ID"),
            client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
            user_agent=os.getenv("REDDIT_USER_AGENT"),
        )
        # Verify it's read-only and connected
        print(f"Reddit client initialized (Read-Only: {reddit_client.read_only})")
        return reddit_client
    except Exception as e:
        print("FATAL ERROR: Could not initialize PRAW client.", file=sys.stderr)
        print(">>> Have you set REDDIT_CLIENT_ID, SECRET, and USER_AGENT in your .env file?", file=sys.stderr)
        print(f"Details: {e}", file=sys.stderr)
        sys.exit(1)


def fetch_latest_posts(reddit_client: praw.Reddit, lookback_hours: int = 48) -> pd.DataFrame:
    """
    Fetches ALL posts from r/nba in the last X hours.
    No keyword filtering - we capture everything and filter in DBT later.
    """
    subreddit_name = "nba"
    subreddit = reddit_client.subreddit(subreddit_name)
    print(f"Fetching all posts from 'r/{subreddit_name}' created in the last {lookback_hours} hours...")

    # 1. Define cutoff (current time - 48 hours in seconds)
    cutoff_time = time.time() - (lookback_hours * 60 * 60)

    posts_data = []

    try:
        # .new(limit=None) iterates indefinitely until we break the loop
        for post in subreddit.new(limit=None):

            # 2. THE CRITICAL CHECK
            # If we hit a post older than 48h, we stop.
            if post.created_utc < cutoff_time:
                print(f"Reached posts older than {lookback_hours} hours. Stopping fetch.")
                break

            # 3. Collect Data
            posts_data.append({
                "post_id": post.id,
                "subreddit": post.subreddit.display_name,
                "title": post.title,
                "score": post.score,
                "num_comments": post.num_comments,
                # Convert unix timestamp to datetime object
                "created_utc": datetime.fromtimestamp(post.created_utc, tz=timezone.utc),
                "url": post.url,
                "selftext": post.selftext,
            })

            # Optional: Print progress every 100 posts
            if len(posts_data) % 100 == 0:
                print(f"Collected {len(posts_data)} posts so far...")

        print(f"Total matching posts found: {len(posts_data)}")
        return pd.DataFrame(posts_data)

    except Exception as e:
        print(f"Error while fetching posts: {e}", file=sys.stderr)
        return pd.DataFrame()


def run_reddit_extraction():
    """Main ETL function."""
    print("--- Starting Reddit Raw Ingestion ---")
    start_time = time.time()

    # 1. Setup Reddit client
    reddit_client = setup_reddit_client()

    # 2. Fetch ALL posts (Last 48 hours)
    # We do NOT filter by player here. We want completeness.
    # DBT will handle the matching logic.
    all_posts_df = fetch_latest_posts(reddit_client, lookback_hours=48)

    # 3. Add load timestamp and load to BQ
    if not all_posts_df.empty:
        all_posts_df["_loaded_at_utc"] = datetime.now(timezone.utc)

        print(f"Uploading {len(all_posts_df)} rows to BigQuery...")

        load_to_bigquery(
            all_posts_df,
            "raw.reddit_nba_posts",
            write_mode="append"
        )
    else:
        print("No posts found in the time window. Skipping BigQuery load.")

    end_time = time.time()
    print(f"\n--- Reddit Ingestion Finished in {end_time - start_time:.2f} seconds ---")


if __name__ == "__main__":
    run_reddit_extraction()