PRAGMA journal_mode=DELETE;
PRAGMA locking_mode=NORMAL;
PRAGMA synchronous=NORMAL;


-- Pages currently in use.
CREATE TABLE IF NOT EXISTS busy_pages (
    id BIGINT PRIMARY KEY
);

-- Pages that are free to be used.
CREATE TABLE IF NOT EXISTS free_pages (
    id BIGINT PRIMARY KEY
);

-- Logical ids
CREATE TABLE IF NOT EXISTS logical_ids (
    logical_id BIGINT PRIMARY KEY,
    page_id BIGINT NOT NULL
);

-- Index on page id
CREATE INDEX IF NOT EXISTS logical_ids_page_id ON logical_ids (page_id);

-- Global information about the database.
CREATE TABLE IF NOT EXISTS global_info (
    unique_row BIGINT PRIMARY KEY,
    next_page_id BIGINT NOT NULL
);

-- Create global info row if it doesn't exist.
INSERT OR IGNORE INTO global_info (unique_row, next_page_id) VALUES (0, 0);