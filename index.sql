CREATE INDEX IF NOT EXISTS blocklist_user_did ON blocklists (user_did);

CREATE INDEX IF NOT EXISTS blocklist_blocked_did ON blocklists (blocked_did);

CREATE INDEX idx_users_did ON users (did);

CREATE INDEX idx_block_date ON blocklists (block_date);

CREATE INDEX idx_users_status_true ON users (status) WHERE status = TRUE;

CREATE INDEX idx_users_status ON users (status);

CREATE INDEX idx_users_status_false ON users (status) WHERE status = FALSE;

CREATE INDEX idx_user_prefixes_prefix1 ON user_prefixes(prefix1);
CREATE INDEX idx_user_prefixes_prefix2 ON user_prefixes(prefix2);
CREATE INDEX idx_user_prefixes_prefix3 ON user_prefixes(prefix3);