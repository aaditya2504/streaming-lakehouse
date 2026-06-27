-- collapse the bronze change-log to the CURRENT state per account,
-- drop deleted accounts, and tokenize the PII column.
with ranked as (
  select *,
         row_number() over (partition by account_id order by ts_ms desc) as rn
  from lake.bronze.accounts
)
select
  account_id,
  to_hex(sha256(to_utf8(customer_name)))  as customer_token,  -- PII masked (one-way hash)
  balance,
  status,
  ts_ms
from ranked
where rn = 1            -- keep only the latest event per account
  and op <> 'd'         -- exclude closed accounts