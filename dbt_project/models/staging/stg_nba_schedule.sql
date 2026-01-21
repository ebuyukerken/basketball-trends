{{
    config(
        materialized='incremental',
        unique_key='game_id',
        incremental_strategy='merge'
    )
}}

WITH source AS (
    SELECT
        game_id,
        game_date,
        utc_start_time,
        _loaded_at_utc
    FROM {{ source('raw', 'nba_schedule') }}

    {% if is_incremental() %}
    WHERE CAST(GAME_DATE AS DATE) >= '{{ var("run_dt") }}'
    {% endif %}
)

SELECT * FROM source
QUALIFY ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY _loaded_at_utc DESC) = 1