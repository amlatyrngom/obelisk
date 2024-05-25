PRAGMA journal_mode=DELETE;
PRAGMA locking_mode=NORMAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS global_info (
    unique_row INTEGER PRIMARY KEY,
    file_size BIGINT NOT NULL,
    next_slab_id INTEGER NOT NULL,
    truncate_lsn BIGINT NOT NULL,
    replicas TEXT DEFAULT NULL
);

INSERT OR IGNORE INTO global_info (unique_row, file_size, next_slab_id, truncate_lsn) VALUES (0, 0, 0, -1);


CREATE TABLE IF NOT EXISTS busy_slabs (
    slab_id INTEGER PRIMARY KEY,
    owner_partition_idx INTEGER NOT NULL,
    highest_persist_lsn BIGINT NOT NULL,
    slab_offset BIGINT NOT NULL
);

-- Create index of partition_idx and highest_persist_lsn for performance.
CREATE INDEX IF NOT EXISTS busy_slabs_idx ON busy_slabs (owner_partition_idx, highest_persist_lsn);


CREATE TABLE IF NOT EXISTS free_slabs (
    slab_id INTEGER PRIMARY KEY,
    slab_offset BIGINT NOT NULL
);