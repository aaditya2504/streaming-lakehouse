CREATE TABLE accounts (
  account_id    SERIAL PRIMARY KEY,
  customer_name TEXT NOT NULL,
  balance       NUMERIC(12,2) NOT NULL DEFAULT 0,
  status        TEXT NOT NULL DEFAULT 'active',
  created_at    TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE transactions (
  txn_id     SERIAL PRIMARY KEY,
  account_id INT NOT NULL REFERENCES accounts(account_id),
  amount     NUMERIC(12,2) NOT NULL,
  txn_type   TEXT NOT NULL,
  ts         TIMESTAMP NOT NULL DEFAULT now()
);

-- CRITICAL: makes UPDATE/DELETE events carry the full old row
ALTER TABLE accounts     REPLICA IDENTITY FULL;
ALTER TABLE transactions REPLICA IDENTITY FULL;