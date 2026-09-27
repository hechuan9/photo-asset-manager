BEGIN TRANSACTION;
CREATE TABLE archive_receipts (
	library_id VARCHAR NOT NULL,
	asset_id VARCHAR(36) NOT NULL,
	receipt_event_seq BIGINT NOT NULL,
	file_object JSON NOT NULL,
	server_placement JSON NOT NULL,
	committed_at DATETIME NOT NULL,
	PRIMARY KEY (library_id, asset_id, receipt_event_seq)
);
CREATE TABLE derivative_objects (
	library_id VARCHAR NOT NULL,
	asset_id VARCHAR(36) NOT NULL,
	role VARCHAR NOT NULL,
	file_object JSON NOT NULL,
	object_bucket VARCHAR NOT NULL,
	object_key VARCHAR NOT NULL,
	object_etag VARCHAR,
	pixel_width BIGINT NOT NULL,
	pixel_height BIGINT NOT NULL,
	declared_event_seq BIGINT NOT NULL,
	updated_at DATETIME NOT NULL,
	PRIMARY KEY (library_id, asset_id, role)
);
CREATE TABLE device_states (
	library_id VARCHAR NOT NULL,
	device_id VARCHAR NOT NULL,
	actor_id VARCHAR NOT NULL,
	last_seen_at DATETIME NOT NULL,
	last_uploaded_device_seq BIGINT NOT NULL,
	last_pull_cursor BIGINT NOT NULL,
	capabilities JSON NOT NULL,
	PRIMARY KEY (library_id, device_id)
);
CREATE TABLE ledger_events (
	library_id VARCHAR NOT NULL,
	global_seq BIGINT NOT NULL,
	op_id VARCHAR(36) NOT NULL,
	device_id VARCHAR NOT NULL,
	device_seq BIGINT NOT NULL,
	hybrid_logical_time JSON NOT NULL,
	actor_id VARCHAR NOT NULL,
	entity_type VARCHAR NOT NULL,
	entity_id VARCHAR NOT NULL,
	op_type VARCHAR NOT NULL,
	payload_json JSON NOT NULL,
	payload_hash VARCHAR(64) NOT NULL,
	base_version VARCHAR,
	committed_at DATETIME NOT NULL,
	PRIMARY KEY (library_id, global_seq),
	CONSTRAINT uq_ledger_events_op_id UNIQUE (op_id),
	CONSTRAINT uq_ledger_events_device_seq UNIQUE (library_id, device_id, device_seq)
);
INSERT INTO "ledger_events" VALUES('library-a',1,'00000000-0000-0000-0000-000000000001','mac',1,'{"wallTimeMilliseconds": 1700000000001, "counter": 0, "nodeID": "mac"}','user','asset','00000000-0000-0000-0000-00000000a001','metadata_set','{"metadataSet": {"assetID": "00000000-0000-0000-0000-00000000a001", "field": "rating", "value": {"int": {"_0": 4}}}}','a5c33ed27a4543e5191cb4e023a067571e22b2c340b3b24c6d73506688b038b0',NULL,'2026-09-26 17:09:14.917281');
CREATE TABLE ledger_sequence_counters (
	library_id VARCHAR NOT NULL,
	next_global_seq BIGINT NOT NULL,
	PRIMARY KEY (library_id)
);
INSERT INTO "ledger_sequence_counters" VALUES('library-a',2);
CREATE TABLE sync_conflicts (
	id VARCHAR(36) NOT NULL,
	library_id VARCHAR NOT NULL,
	entity_type VARCHAR NOT NULL,
	entity_id VARCHAR NOT NULL,
	conflict_type VARCHAR NOT NULL,
	left_op_id VARCHAR(36),
	right_op_id VARCHAR(36),
	detail JSON NOT NULL,
	created_at DATETIME NOT NULL,
	PRIMARY KEY (id)
);
CREATE INDEX ix_ledger_events_entity ON ledger_events (library_id, entity_type, entity_id, global_seq);
CREATE INDEX ix_ledger_events_op_type ON ledger_events (library_id, op_type, global_seq);
COMMIT;
