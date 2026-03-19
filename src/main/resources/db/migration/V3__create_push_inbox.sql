CREATE TABLE push_inbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    key VARCHAR(255) NOT NULL,
    value TEXT NOT NULL,

    topic VARCHAR(255) NOT NULL,

    processed BOOLEAN NOT NULL DEFAULT FALSE,
    attempt INTEGER NOT NULL DEFAULT 0,

    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX push_inbox_key_value_idx
    ON push_inbox (key, value);