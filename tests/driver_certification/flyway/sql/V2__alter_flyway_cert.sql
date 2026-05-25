ALTER TABLE flyway_cert ADD COLUMN applied_by TEXT;

UPDATE flyway_cert SET applied_by = 'flyway' WHERE id = 1;
