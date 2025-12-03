{{
    config(
        materialized='table',
        cluster_by = ["player_id"]
    )
}}

WITH source AS (
    SELECT * FROM {{ source('raw', 'nba_player_list') }}
),

renamed AS (
    SELECT
        CAST(PERSON_ID AS STRING) AS player_id,
        DISPLAY_FIRST_LAST AS full_name,
        DISPLAY_LAST_COMMA_FIRST AS last_first,
        CAST(FROM_YEAR AS INTEGER) AS start_year,
        CAST(TO_YEAR AS INTEGER) AS end_year,
        PLAYERCODE AS player_slug,
        _loaded_at_utc
    FROM source
)

SELECT * FROM renamed
QUALIFY ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY _loaded_at_utc DESC) = 1