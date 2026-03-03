{{
    config(
        materialized='table',
        partition_by={
            "field": "game_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by = ["player_name", "team"]
    )
}}

WITH game_stats AS (
    SELECT
        gl.season_year,
        gl.game_date,
        ns.utc_start_time AS game_start_utc,
        IFNULL(
            LEAD(ns.utc_start_time) OVER(PARTITION BY gl.player_name ORDER BY ns.utc_start_time),
            DATE_ADD(ns.utc_start_time, INTERVAL 7 DAY)
        ) AS next_game_start_utc,
        gl.player_name,
        gl.team_abbreviation AS team,
        TRIM(
            REPLACE(
                REPLACE(
                    REPLACE(gl.matchup, gl.team_abbreviation, ''), -- 1. Remove home team
                    'vs.', ''                                      -- 2. Remove 'vs.'
                ),
                '@', ''                                            -- 3. Remove '@'
            )
        ) AS opponent_team,
        CASE
            WHEN gl.matchup LIKE '%@%' THEN 'away'
            ELSE 'home'
        END AS travel_status,
        gl.result,
        gl.minutes_played,
        gl.fgm,
        gl.fga,
        gl.fg_pct,
        gl.ftm,
        gl.fta,
        gl.ft_pct,
        gl.oreb,
        gl.dreb,
        gl.ast,
        gl.tov,
        gl.stl,
        gl.blk,
        gl.pf,
        gl.pts,
        gl.plus_minus,
        gl.nba_fantasy_pts,
        gl.seconds_played
    FROM {{ ref('stg_nba_game_logs') }} AS gl
    LEFT JOIN {{ ref('stg_nba_schedule') }} AS ns
        ON gl.game_id = ns.game_id
    WHERE gl.game_date >= '2025-09-01'
),

reddit_stats AS (
    SELECT
        matched_player,
        post_type,
        created_date,
        created_utc,
        COUNT(DISTINCT title) AS posts_count,
        SUM(num_comments) AS total_comments,
        SUM(score) AS total_score
    FROM {{ ref('int_reddit_player_mentions') }}
    WHERE created_date >= '2026-01-01'
    GROUP BY 1, 2, 3, 4
)

SELECT
    gs.season_year,
    gs.game_date,
    gs.game_start_utc,
    gs.player_name,
    gs.team,
    gs.opponent_team,
    gs.travel_status,
    gs.result,
    gs.minutes_played,
    gs.fgm,
    gs.fga,
    gs.fg_pct,
    gs.ftm,
    gs.fta,
    gs.ft_pct,
    gs.oreb,
    gs.dreb,
    gs.ast,
    gs.tov,
    gs.stl,
    gs.blk,
    gs.pf,
    gs.pts,
    gs.plus_minus,
    gs.nba_fantasy_pts,
    gs.seconds_played,
    rs.post_type,
    SUM(rs.posts_count) AS post_count,
    SUM(rs.total_comments) AS total_comments,
    SUM(rs.total_score) AS total_score
FROM game_stats AS gs
INNER JOIN reddit_stats AS rs
    ON gs.player_name = rs.matched_player
    AND rs.created_utc >= gs.game_start_utc
    AND rs.created_utc < gs.next_game_start_utc
GROUP BY ALL