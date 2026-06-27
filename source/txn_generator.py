import os, random, time, psycopg2

NAMES = ["Ava Patel","Liam Chen","Noah Kim","Mia Singh","Ada Roy","Eli Das","Zoe Bose","Ravi Nair"]
TYPES = ["deposit","withdrawal","transfer","fee"]

conn = psycopg2.connect(
    host="localhost", port=5432, dbname="banking", user="bank", password="bankpw"
)
conn.autocommit = True
cur = conn.cursor()

def seed_accounts(n=8):
    for nm in NAMES[:n]:
        cur.execute(
            "INSERT INTO accounts (customer_name, balance) VALUES (%s, %s)",
            (nm, round(random.uniform(100, 5000), 2)),
        )

def account_ids():
    cur.execute("SELECT account_id FROM accounts WHERE status='active'")
    return [r[0] for r in cur.fetchall()]

seed_accounts()
print("seeded accounts; generating changes (Ctrl-C to stop)...")

while True:
    ids = account_ids()
    if not ids:
        seed_accounts(); continue
    roll = random.random()
    if roll < 0.70:                                   # INSERT a transaction + move balance
        aid = random.choice(ids)
        amt = round(random.uniform(5, 800), 2)
        typ = random.choice(TYPES)
        cur.execute(
            "INSERT INTO transactions (account_id, amount, txn_type) VALUES (%s,%s,%s)",
            (aid, amt, typ),
        )
        delta = amt if typ == "deposit" else -amt
        cur.execute("UPDATE accounts SET balance = balance + %s WHERE account_id=%s", (delta, aid))
    elif roll < 0.90:                                 # UPDATE status (flip active/frozen)
        aid = random.choice(ids)
        cur.execute("UPDATE accounts SET status='frozen' WHERE account_id=%s", (aid,))
    else:                                             # DELETE (close an account)
        aid = random.choice(ids)
        cur.execute("DELETE FROM transactions WHERE account_id=%s", (aid,))
        cur.execute("DELETE FROM accounts WHERE account_id=%s", (aid,))
    time.sleep(1)