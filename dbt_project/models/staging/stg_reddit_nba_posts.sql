{{
    config(
        materialized='incremental',
        unique_key='post_id',
        incremental_strategy='insert_overwrite',
        partition_by={
            "field": "created_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by = ["post_id"]
    )
}}

WITH source AS (
    SELECT * FROM {{ source('raw', 'reddit_nba_posts') }}

    {% if is_incremental() %}
    --ingestion: posts from the last 48h. dbt run in every 24h. DATE_SUB to get the most current post stats (upvotes, comments)
    --otherwise immature posts will be misleading. e.g. the post shared just a few minutes before the ingestion.
    --so the same posts will be fetched again 24 hours later. upvotes and comments will be updated.
    --deduplication in the final part
    WHERE DATE(created_utc) >= DATE_SUB('{{ var("run_dt") }}', INTERVAL 1 DAY)
    {% endif %}
),

renamed AS (
    SELECT
        post_id,
        subreddit,
        title,
        score,
        num_comments,
        url,
        selftext AS body,

        -- Timestamps
        CAST(created_utc AS TIMESTAMP) as created_utc,
        -- Extract the date for partitioning
        DATE(created_utc) AS created_date,

        _loaded_at_utc

    FROM source
)

SELECT * FROM renamed
-- Deduplication:
-- We might scrape the same post multiple times as comments grow.
-- We want the very last snapshot of the post.
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY post_id
    ORDER BY _loaded_at_utc DESC
) = 1