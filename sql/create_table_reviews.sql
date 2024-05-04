CREATE TABLE IF NOT EXISTS reviews (
    id INTEGER PRIMARY KEY,
    restaurant_id INTEGER,
    review_id INTEGER,
    text TEXT,
    rating REAL,
    published_at TEXT
);