CREATE VIEW IF NOT EXISTS view_processed_reviews AS
WITH cte_reviews_deduplicated AS (
    WITH cte_row_numbers AS (
        SELECT
            *,
            ROW_NUMBER() OVER(PARTITION BY restaurant_id, review_id, published_at ORDER BY published_at DESC) rn
        FROM reviews
    )
    SELECT
        restaurant_id,
        review_id,
        text,
        rating,
        published_at
    FROM cte_row_numbers
    WHERE rn = 1
),

cte_swear_word_count AS (
    SELECT
        *,
        COALESCE(((LENGTH(text) - LENGTH(REPLACE(text, '*****', ''))) / LENGTH('*****')), 0) AS swear_word_count
    FROM cte_reviews_deduplicated
),

cte_detect_updates AS (
    WITH cte_records as (
    SELECT
        restaurant_id,
        review_id,
        text,
        rating,
        published_at,
        swear_word_count,
        --FIX LAST UPDATED
        ROW_NUMBER() OVER(PARTITION BY restaurant_id, review_id ORDER BY published_at DESC) rn
    FROM cte_swear_word_count
    )
    SELECT
        restaurant_id,
        review_id,
        text,
        rating,
        published_at,
        swear_word_count
    FROM cte_records
    WHERE rn = 1
),

cte_reviews_filtered AS (
    SELECT 
        *,
        CASE
            WHEN swear_word_count > 0 THEN ROUND((swear_word_count * 1.0 / (LENGTH(text) - LENGTH(REPLACE(text, ' ', '')) + 1)),2)
            ELSE 0
        END as swear_word_ratio,
        CASE
            WHEN published_at < strftime('%Y-%m-%dT%H:%M:%S', 'now', '-1 years') THEN TRUE
            ELSE FALSE
        END as is_outdated
    FROM cte_detect_updates
)

SELECT
    restaurant_id,
    review_id,
    text,
    rating,
    published_at,
    swear_word_ratio,
    is_outdated
FROM cte_reviews_filtered;




