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
    WHERE DATE(created_utc) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
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