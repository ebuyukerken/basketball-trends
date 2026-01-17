# File: scripts/common/bigquery_loader.py


import os
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import sys
from pathlib import Path

# --- Configuration ---
# 1. Set the path to the Service Account JSON file
# 'gcp_service_account.json' file is in the PROJECT ROOT, one level above the 'scripts' directory.
try:
    env_key_path = os.getenv('GCP_KEY_PATH')

    if env_key_path:
        # We are likely in Docker or have set the var explicitly
        KEY_PATH = Path(env_key_path)
    else:
        # Fallback: We are running locally (no env var set)
        CURRENT_DIR = Path(__file__).resolve().parent
        PROJECT_ROOT = CURRENT_DIR.parent.parent
        KEY_PATH = PROJECT_ROOT / "gcp_service_account.json"

    # 2. Authenticate
    # Check if the key file exists
    if not KEY_PATH.exists():
        raise FileNotFoundError(
            f"Service Account Key not found at: {KEY_PATH}\n"
            f"Please ensure 'gcp_service_account.json' is in the project root directory."
        )

    # 3. Create a BigQuery client with these credentials
    CREDENTIALS = service_account.Credentials.from_service_account_file(KEY_PATH)
    BQ_CLIENT = bigquery.Client(credentials=CREDENTIALS, project=CREDENTIALS.project_id)
    PROJECT_ID = CREDENTIALS.project_id
    print(f"BigQuery loader initialized. Project: {CREDENTIALS.project_id}")

except Exception as e:
    print(f"FATAL ERROR: Could not initialize BigQuery client.", file=sys.stderr)
    print(f"Details: {e}", file=sys.stderr)
    # If the client fails to init, we can't do anything else.
    sys.exit(1)


# --- End Configuration ---


def load_to_bigquery(dataframe: pd.DataFrame, table_id: str, write_mode: str = "overwrite"):
    """
    Loads a Pandas DataFrame into a specified BigQuery table.
    ...
    """
    if dataframe.empty:
        print(f"Skipping load to {table_id}: DataFrame is empty.")
        return

    # --- Set Load Job Configuration ---
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True  # Auto-detect schema for all jobs.

    # Set the write disposition
    if write_mode == "overwrite":
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    elif write_mode == "append":
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND


        # Schema updates are ONLY allowed when appending.
        job_config.schema_update_options = [
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        ]
    else:
        print(f"Warning: Invalid write_mode '{write_mode}'. Defaulting to 'overwrite'.", file=sys.stderr)
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE


    try:
        # --- Start the Load Job ---
        print(f"Starting BigQuery load job for '{table_id}' (Mode: {write_mode})...")
        job = BQ_CLIENT.load_table_from_dataframe(
            dataframe, table_id, job_config=job_config
        )

        # Wait for the job to complete
        job.result()

        # --- Job Succeeded ---
        print(f"Successfully loaded {len(dataframe)} rows into '{table_id}'.")

    except Exception as e:
        # --- Job Failed ---
        print(f"ERROR: BigQuery load job failed for '{table_id}'.", file=sys.stderr)
        print(f"Details: {e}", file=sys.stderr)

        if 'job' in locals() and hasattr(job, 'errors'):
            print(f"Job Errors: {job.errors}", file=sys.stderr)

# Connection test
if __name__ == "__main__":
    print("\n--- Testing BigQuery Loader Connection ---")

    # 1. Create a dummy DataFrame
    test_df = pd.DataFrame(
        data={"col1": [1, 2], "col2": ["a", "b"]},
        index=[1, 2]  # Use a non-default index to ensure it's handled
    )
    test_table_id = f"{CREDENTIALS.project_id}.raw.loader_test"

    print(f"Creating test DataFrame:\n{test_df}")
    print(f"Target test table: {test_table_id}")

    # 2. Run the load function (overwrite)
    load_to_bigquery(test_df, test_table_id, write_mode="overwrite")

    # 3. Run the load function (append)
    load_to_bigquery(test_df, test_table_id, write_mode="append")

    print("\nTest complete. Check your BigQuery 'raw' dataset for a table")
    print("named 'loader_test' with 4 rows.")