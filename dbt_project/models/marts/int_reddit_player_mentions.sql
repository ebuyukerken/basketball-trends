{{
    config(
        materialized='incremental',
        unique_key=['post_id', 'matched_player'],
        incremental_strategy='merge',
        partition_by={
            "field": "created_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by = ["matched_player", "post_id"]
    )
}}

/*
merge instead of insert_overwrite.
insert_overwrite fails in some cases. example:
lebron played on Dec 5. there are posts from Dec 5 and Dec 6.
run_dt: Dec 5. lebron played, exists in the player list. posts are matched.
run_dt: Dec 6. lebron didn't play. does not exist in the player list.
posts on 6th are not matched with lebron.
6th is overwritten (insert overwrite). lebron posts on 6th are missed.
merge does not overwrite in this case.
*/

WITH
-- unique player list
player_list AS (
    SELECT DISTINCT
        player_name AS original_name,
        LOWER(player_name) AS match_name
    FROM {{ ref('stg_nba_game_logs') }}
    {% if is_incremental() %}
    WHERE game_date >= '{{ var("run_dt") }}'
    {% endif %}
),

-- clean and tokenize the posts
posts_tokenized AS (
    SELECT
        post_id,
        title,
        subreddit,
        url,
        body,
        num_comments,
        score,
        created_utc,
        created_date,
        -- remove punctuation but keep hyphens for names like "Karl-Anthony"
        -- split into array of words
        -- generate combinations of 2 and 3 words
        ML.NGRAMS(
            SPLIT(REGEXP_REPLACE(title, r'[^\w\s-]', ''), ' '),
            [2, 3],
            ' '
        ) AS ngrams
    FROM {{ ref('stg_reddit_nba_posts') }}
    {% if is_incremental() %}
    WHERE created_date >= '{{ var("run_dt") }}'
    {% endif %}
)

-- match Tokens to Players
SELECT
    p.post_id,
    p.subreddit,
    p.title,
    pl.original_name AS matched_player,
    p.url,
    p.body,
    p.created_utc,
    p.created_date,
    MAX(p.num_comments) AS num_comments,
    MAX(p.score) AS score,
    CASE WHEN p.title LIKE '%[Highlight]%' THEN 'Highlight'
        WHEN p.title LIKE '%[Post Game Thread]%' THEN 'Post Game Thread'
        ELSE 'Other' END AS post_type
FROM posts_tokenized p
-- flatten the N-Grams array into rows
CROSS JOIN UNNEST(p.ngrams) AS potential_name
-- inner join with player list
INNER JOIN player_list pl
    ON LOWER(potential_name) = pl.match_name
-- deduplicate if a player is mentioned twice in one title
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 11