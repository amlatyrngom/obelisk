PRAGMA journal_mode=DELETE;
PRAGMA locking_mode=NORMAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS lease (
    unique_row INTEGER PRIMARY KEY,
    incarnation_num BIGINT NOT NULL,
    expiry BIGINT NOT NULL,
    waiting BIGINT NOT NULL
);

INSERT OR IGNORE INTO lease (unique_row, incarnation_num, expiry, waiting) VALUES (0, 0, 0, -10);