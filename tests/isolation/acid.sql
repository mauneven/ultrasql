-- UltraSQL-authored ACID transfer baseline.
-- This is a small account-transfer invariant script, not an imported
-- third-party SQL dump. The test harness checks that committed transfers and
-- rolled-back partial transfers preserve total balance.

CREATE TABLE isolation_acid_accounts (id INT NOT NULL, balance INT NOT NULL);
INSERT INTO isolation_acid_accounts VALUES (1, 0), (2, 0);

BEGIN;
UPDATE isolation_acid_accounts SET balance = balance + 50 WHERE id = 1;
UPDATE isolation_acid_accounts SET balance = balance - 50 WHERE id = 2;
COMMIT;

BEGIN;
UPDATE isolation_acid_accounts SET balance = balance + 25 WHERE id = 1;
ROLLBACK;
