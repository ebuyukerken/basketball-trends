# File: scripts/extract_reddit.py

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


def get_players_from_bq(yesterday_str: str) -> list:
    """
    Queries BigQuery to get a list of players who played yesterday.
    """
    print(f"Querying BigQuery for players who played on {yesterday_str}...")
    query = f"""
        SELECT DISTINCT PLAYER_NAME 
        FROM `{PROJECT_ID}.raw.nba_game_logs`
        WHERE CAST(GAME_DATE AS DATE) = DATE('{yesterday_str}')
    """
    try:
        query_job = BQ_CLIENT.query(query)
        df = query_job.to_dataframe()
        players = df['PLAYER_NAME'].dropna().tolist()
        print(f"Found {len(players)} players from game logs.")
        return players
    except Exception as e:
        print(f"Error querying BigQuery for players: {e}", file=sys.stderr)
        return []


def load_keywords_from_seed() -> list:
    """
    Loads the 'alias' column from the seed file.
    """
    PROJECT_ROOT = SCRIPT_DIR.parent
    SEED_FILE_PATH = PROJECT_ROOT / "dbt_project" / "seeds" / "seed_player_aliases.csv"
    try:
        df = pd.read_csv(SEED_FILE_PATH)
        aliases = df['alias'].dropna().unique().tolist()
        print(f"Loaded {len(aliases)} unique aliases from seed file.")
        return aliases
    except FileNotFoundError:
        print(f"Warning: Seed file not found at {SEED_FILE_PATH}. Proceeding without aliases.", file=sys.stderr)
        return []


def build_search_query(players: list, aliases: list) -> str:
    """
    Combines player names and aliases into a single,
    robust OR-separated search query for Reddit.
    """
    all_keywords = set(players) | set(aliases)

    # Escape double-quotes in names
    # Wrap all multi-word phrases in quotes
    safe_keywords = []
    for kw in all_keywords:
        if ' ' in kw:
            # Wrap phrases in quotes
            safe_keywords.append(f'"{kw.replace("\"", "")}"')  # Remove internal quotes
        else:
            # Single words are fine
            safe_keywords.append(kw)

    # PRAW's search uses Lucene syntax. "OR" is the operator.
    query_string = " OR ".join(safe_keywords)
    print(f"Built 1 batched search query with {len(all_keywords)} total keywords.")
    return query_string


def fetch_reddit_posts(reddit_client: praw.Reddit, query_string: str) -> pd.DataFrame:
    """
    Makes a single, batched search query to r/nba.
    """
    subreddit = reddit_client.subreddit("nba")
    print(f"Searching 'r/nba' for 'new' posts in the last 'day'...")

    # We make ONE search call, as requested by Reddit's API rules
    # time_filter='day' gets posts from the last 24 hours
    submissions = subreddit.search(
        query=query_string,
        sort="new",
        time_filter="day"
    )

    posts_data = []
    try:
        for post in submissions:
            posts_data.append({
                "post_id": post.id,
                "title": post.title,
                "score": post.score,
                "num_comments": post.num_comments,
                "created_utc": datetime.fromtimestamp(post.created_utc, tz=timezone.utc),
                "url": post.url,
                "selftext": post.selftext,
            })

        print(f"Found {len(posts_data)} matching posts.")
        return pd.DataFrame(posts_data)

    except Exception as e:
        print(f"Error while fetching posts: {e}", file=sys.stderr)
        return pd.DataFrame()

def batch_keywords(keywords: set, batch_size: int = 15):
    """Splits the set of keywords into smaller lists (batches)."""
    keyword_list = list(keywords)
    for i in range(0, len(keyword_list), batch_size):
        yield keyword_list[i:i + batch_size]


def run_reddit_extraction():
    """Main ETL function, now with batching."""
    print("--- Starting Reddit Hype Extraction ---")
    start_time = time.time()

    # 0. Get yesterday's date string
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")

    # 1. Setup Reddit client
    reddit_client = setup_reddit_client()

    # 2. Get dynamic player list from BQ
    players_list = get_players_from_bq(yesterday_str)

    # 3. Load static alias list
    alias_list = load_keywords_from_seed()

    if not players_list and not alias_list:
        print("No players or aliases found. Nothing to search. Exiting.")
        return

    # 4. Combine all keywords into one set
    all_keywords = set(players_list) | set(alias_list)
    print(f"Total unique keywords to process: {len(all_keywords)}")

    # 5. Fetch posts IN BATCHES
    all_posts_df = pd.DataFrame()

    for i, keyword_batch in enumerate(batch_keywords(all_keywords, batch_size=15)):

        # A) Build a smaller query for just this batch
        # (We pass an empty list [] for aliases since we're already batched)
        batch_query_string = build_search_query(keyword_batch, [])

        print(f"\n--- Batch {i + 1}: Fetching {len(keyword_batch)} keywords ---")
        # print(batch_query_string) # Optional: print each query

        # B) Fetch posts for this small batch
        posts_df = fetch_reddit_posts(reddit_client, batch_query_string)

        # C) Add to our master list
        if not posts_df.empty:
            all_posts_df = pd.concat([all_posts_df, posts_df], ignore_index=True)

        # D) Be polite to the API. 1 second is fine.
        time.sleep(1)

        # 6. De-duplicate (since one post might match multiple batches)
    if not all_posts_df.empty:
        print(f"\nFound {len(all_posts_df)} total posts before deduplication.")
        all_posts_df = all_posts_df.drop_duplicates(subset=['post_id'])
        print(f"Found {len(all_posts_df)} unique posts after deduplication.")

    # 7. Add load timestamp and load to BQ
    if not all_posts_df.empty:
        all_posts_df["_loaded_at_utc"] = datetime.now(timezone.utc)
        load_to_bigquery(
            all_posts_df,
            "raw.reddit_nba_posts",
            write_mode="overwrite"
        )
    else:
        print("No posts found. Skipping BigQuery load.")

    end_time = time.time()
    print(f"\n--- Reddit Extraction Finished in {end_time - start_time:.2f} seconds ---")

if __name__ == "__main__":
    run_reddit_extraction()