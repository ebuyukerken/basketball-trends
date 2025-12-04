{{
    config(
        materialized='incremental',
        unique_key=['game_id', 'player_id'],
        incremental_strategy='insert_overwrite',
        partition_by={
            "field": "game_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by = ["player_id", "team_id"]
    )
}}

WITH source AS (
    SELECT
        *
    FROM {{ source('raw', 'nba_game_logs') }}
    {% if is_incremental() %}
    WHERE CAST(GAME_DATE AS DATE) >= '{{ var("run_dt") }}'
    {% endif %}
),

renamed AS (
    SELECT
        -- IDs
        CAST(GAME_ID AS STRING) AS game_id,
        CAST(PLAYER_ID AS STRING) AS player_id,
        CAST(TEAM_ID AS STRING) AS team_id,

        -- Dimensions
        SEASON_YEAR AS season_year,
        PLAYER_NAME AS player_name,
        TEAM_ABBREVIATION AS team_abbreviation,
        TEAM_NAME AS team_name,
        MATCHUP AS matchup,
        WL AS result,

        -- Dates
        CAST(GAME_DATE AS DATE) AS game_date,

        -- Stats
        MIN AS minutes_played,
        FGM AS fgm,
        FGA AS fga,
        FG_PCT AS fg_pct,
        FG3M AS fg3m,
        FG3A AS fg3a,
        FG3_PCT AS fg3_pct,
        FTM AS ftm,
        FTA AS fta,
        FT_PCT AS ft_pct,
        OREB AS oreb,
        DREB AS dreb,
        REB AS reb,
        AST AS ast,
        TOV AS tov,
        STL AS stl,
        BLK AS blk,
        BLKA AS blka,
        PF AS pf,
        PFD AS pfd,
        PTS AS pts,
        PLUS_MINUS AS plus_minus,
        NBA_FANTASY_PTS AS nba_fantasy_pts,
        DD2 AS dd2,
        TD3 AS td3,
        WNBA_FANTASY_PTS AS wnba_fantasy_pts,
        IFNULL(
            CAST(SPLIT(MIN_SEC, ':')[OFFSET(0)] AS INT64) * 60 +
            CAST(SPLIT(MIN_SEC, ':')[OFFSET(1)] AS INT64),
            0
        ) AS seconds_played,

        -- Metadata
        _loaded_at_utc

    FROM source
)

SELECT
    *
FROM renamed
QUALIFY ROW_NUMBER() OVER (PARTITION BY game_id, player_id ORDER BY _loaded_at_utc DESC) = 1