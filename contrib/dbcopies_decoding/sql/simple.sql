-- predictability
SET synchronous_commit = on;

CREATE TABLE replication_example(id SERIAL PRIMARY KEY, somedata int, text varchar(120));

SELECT 'init' FROM pg_create_logical_replication_slot('dbcopies_slot', 'dbcopies_decoding');

BEGIN;
INSERT INTO replication_example(somedata, text) VALUES (1, 1);
INSERT INTO replication_example(somedata, text) VALUES (1, 2);
COMMIT;

SELECT data FROM pg_logical_slot_get_changes('dbcopies_slot', NULL, NULL, 'include-xids', '0');

SELECT pg_drop_replication_slot('dbcopies_slot');
