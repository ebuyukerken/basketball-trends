{{
    config(
        enabled=false,
        materialized='incremental',
        unique_key='game_id',
        incremental_strategy='merge'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('raw', 'nba_schedule') }}

    {% if is_incremental() %}
    -- Only fetch schedule data loaded since the last run
    WHERE _loaded_at_utc > (SELECT COALESCE(MAX(_loaded_at_utc), '1900-01-01') FROM {{ this }})
    {% endif %}
),

renamed AS (
    SELECT
        CAST(GAME_ID AS STRING) AS game_id,
        game_start_utc AS game_start_utc,
        game_time_et_str AS game_time_et,
        _loaded_at_utc
    FROM source
)

SELECT * FROM renamed
QUALIFY ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY _loaded_at_utc DESC) = 1