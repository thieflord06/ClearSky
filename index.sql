CREATE INDEX IF NOT EXISTS blocklist_user_did ON blocklists (user_did);

CREATE INDEX IF NOT EXISTS blocklist_blocked_did ON blocklists (blocked_did);

CREATE INDEX idx_user_did_blocked_did ON blocklists (user_did, blocked_did);

CREATE INDEX idx_block_date ON blocklists (block_date);

CREATE EXTENSION pg_trgm;

CREATE INDEX idx_users_handle_fulltext ON users USING gin (handle gin_trgm_ops);

CREATE INDEX idx_users_did ON users (did);

CREATE INDEX idx_users_handle ON users (handle);

CREATE INDEX idx_users_status ON users (status);

CREATE INDEX idx_users_status_true ON users (status) WHERE status = TRUE;

CREATE INDEX idx_users_status_false ON users (status) WHERE status = FALSE;

CREATE INDEX idx_mutelists_uri ON mutelists (uri);

CREATE INDEX idx_mutelists_users_list ON mutelists_users (list);

CREATE INDEX idx_mutelists_users_did ON mutelists_users (did);

CREATE INDEX users_pds_index ON users (pds);

CREATE INDEX unique_did_list_type ON top_block (did, list_type);

CREATE INDEX unique_did_list_type_24 ON top_twentyfour_hour_block (did, list_type);