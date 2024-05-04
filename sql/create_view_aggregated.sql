CREATE VIEW IF NOT EXISTS view_aggregated_reviews AS
WITH cte_enriched AS (
    SELECT
        *,
        CAST((julianday('now') - julianday(published_at)) as INTEGER) as review_age,
        LENGTH(text) as review_length
    FROM view_processed_reviews
)
SELECT
    restaurant_id,
    COUNT(review_id) as review_count,
    ROUND(AVG(rating), 2) as average_rating,
    CAST(AVG(review_length) as INTEGER) as average_review_length,
    json_object(
        'oldest', CAST(MAX(review_age) as INTEGER),
        'newest', CAST(MIN(review_age) as INTEGER),
        'average', CAST(AVG(review_age) as INTEGER)
    ) as review_age
FROM cte_enriched
GROUP BY restaurant_id;