-- GOLD: portfolio summary by account status.
with current_accounts as (
    select account_id, balance, status
    from {{ ref('accounts_current') }}
    where balance is not null
)
select
    status,
    count(*)                              as account_count,
    cast(sum(balance) as decimal(18, 2))  as total_balance,
    cast(avg(balance) as decimal(18, 2))  as avg_balance,
    cast(min(balance) as decimal(18, 2))  as min_balance,
    cast(max(balance) as decimal(18, 2))  as max_balance
from current_accounts
group by status
order by total_balance desc
