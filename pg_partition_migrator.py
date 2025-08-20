#!/usr/bin/env python3
"""
pg_partition_migrator.py

Zero/near-zero downtime migration helper to move a large non-partitioned table
(customers) to a range-partitioned table by id in PostgreSQL.

Key features
------------
- Creates a new partitioned table `customers_new` with RANGE partitions on id.
- Dual-write triggers on the source table (`customers`) keep the new table in sync during backfill.
- Batched backfill to control load.
- Validation (row counts + checksum).
- Cutover that atomically renames tables with a brief lock window.
- Safety checks for foreign keys referencing the source (abort unless --force).
- Replays GRANT privileges from the original table to the new one at cutover.

Usage
-----
Set your connection via environment variables (or pass a DSN):
  export PGHOST=localhost PGPORT=5432 PGUSER=postgres PGPASSWORD=secret PGDATABASE=mydb

Dry run to see the plan:
  python3 pg_partition_migrator.py --dry-run

Create partitions (8 by default), backfill, validate, and cut over:
  python3 pg_partition_migrator.py --run-all --partitions 8 --batch-size 100000

Steps can also be run individually:
  python3 pg_partition_migrator.py --prepare
  python3 pg_partition_migrator.py --backfill
  python3 pg_partition_migrator.py --validate
  python3 pg_partition_migrator.py --cutover

Notes
-----
- This script assumes the source table is:
    CREATE SEQUENCE customers_id_seq;
    CREATE TABLE customers (
        id BIGINT PRIMARY KEY DEFAULT nextval('customers_id_seq'),
        name VARCHAR(255),
        email VARCHAR(255)
    );
- Adjust partition count/ranges with --partitions/--range-size or provide explicit ranges via --ranges.
- If there are FKs referencing customers, the script will warn and abort unless --force is set.
- Test in staging before production. Read the code and understand each step.
"""

import argparse
import os
import sys
import time
import math
import hashlib
from typing import List, Tuple, Optional

import psycopg2
import psycopg2.extras

# -------------- Helpers --------------

def dsn_from_env(args):
    if args.dsn:
        return args.dsn
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    user = os.getenv("PGUSER", "postgres")
    password = os.getenv("PGPASSWORD", "")
    db = os.getenv("PGDATABASE", "postgres")
    return f"host={host} port={port} user={user} password={password} dbname={db}"

def fetchone(cur, sql, params=None):
    cur.execute(sql, params or ())
    return cur.fetchone()

def fetchall(cur, sql, params=None):
    cur.execute(sql, params or ())
    return cur.fetchall()

def execute(cur, sql, params=None):
    cur.execute(sql, params or ())

def advisory_lock(conn, key=834233210155):
    with conn.cursor() as cur:
        execute(cur, "SELECT pg_try_advisory_lock(%s);", (key,))
        locked = cur.fetchone()[0]
        if not locked:
            raise RuntimeError("Could not acquire advisory lock; is another migration running?")
        conn.commit()

def advisory_unlock(conn, key=834233210155):
    with conn.cursor() as cur:
        execute(cur, "SELECT pg_advisory_unlock(%s);", (key,))
        conn.commit()

def qualified(relname: str) -> str:
    # allow schema-qualified names if provided; default to public.
    if "." in relname:
        return relname
    return f"public.{relname}"

# -------------- SQL Templates --------------

TRIGGER_FN = r"""
CREATE OR REPLACE FUNCTION {src}_dualwrite_ins() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO {dst} (id, name, email)
  VALUES (NEW.id, NEW.name, NEW.email)
  ON CONFLICT (id) DO UPDATE
  SET name = EXCLUDED.name, email = EXCLUDED.email;
  RETURN NEW;
END; $$;

CREATE OR REPLACE FUNCTION {src}_dualwrite_upd() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  -- Upsert to the new table. If id changed, delete old row first.
  IF NEW.id = OLD.id THEN
    INSERT INTO {dst} (id, name, email)
    VALUES (NEW.id, NEW.name, NEW.email)
    ON CONFLICT (id) DO UPDATE
    SET name = EXCLUDED.name, email = EXCLUDED.email;
  ELSE
    DELETE FROM {dst} WHERE id = OLD.id;
    INSERT INTO {dst} (id, name, email) VALUES (NEW.id, NEW.name, NEW.email)
    ON CONFLICT (id) DO UPDATE
    SET name = EXCLUDED.name, email = EXCLUDED.email;
  END IF;
  RETURN NEW;
END; $$;

CREATE OR REPLACE FUNCTION {src}_dualwrite_del() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  DELETE FROM {dst} WHERE id = OLD.id;
  RETURN OLD;
END; $$;
"""

TRIGGERS = r"""
DROP TRIGGER IF EXISTS {src}_ins_dual ON {src};
DROP TRIGGER IF EXISTS {src}_upd_dual ON {src};
DROP TRIGGER IF EXISTS {src}_del_dual ON {src};

CREATE TRIGGER {src}_ins_dual AFTER INSERT ON {src}
FOR EACH ROW EXECUTE FUNCTION {src}_dualwrite_ins();

CREATE TRIGGER {src}_upd_dual AFTER UPDATE ON {src}
FOR EACH ROW EXECUTE FUNCTION {src}_dualwrite_upd();

CREATE TRIGGER {src}_del_dual AFTER DELETE ON {src}
FOR EACH ROW EXECUTE FUNCTION {src}_dualwrite_del();
"""

DROP_TRIGGERS = r"""
DROP TRIGGER IF EXISTS {src}_ins_dual ON {src};
DROP TRIGGER IF EXISTS {src}_upd_dual ON {src};
DROP TRIGGER IF EXISTS {src}_del_dual ON {src};

DROP FUNCTION IF EXISTS {src}_dualwrite_ins();
DROP FUNCTION IF EXISTS {src}_dualwrite_upd();
DROP FUNCTION IF EXISTS {src}_dualwrite_del();
"""

CREATE_PARTITIONED_PARENT = r"""
-- Parent partitioned table
CREATE TABLE IF NOT EXISTS {dst} (
  id BIGINT PRIMARY KEY DEFAULT nextval('customers_id_seq'),
  name VARCHAR(255),
  email VARCHAR(255)
) PARTITION BY RANGE (id);
"""

CREATE_CHILD_PARTITION = r"""
CREATE TABLE IF NOT EXISTS {dst}_p_{from_id}_{to_id}
  PARTITION OF {dst}
  FOR VALUES FROM ({from_id}) TO ({to_id});
"""

CREATE_CHILD_MAXPART = r"""
CREATE TABLE IF NOT EXISTS {dst}_p_{from_id}_max
  PARTITION OF {dst}
  FOR VALUES FROM ({from_id}) TO (MAXVALUE);
"""

CHECK_MAX_ID = "SELECT COALESCE(MAX(id), 0) FROM {src};"
CHECK_MIN_ID = "SELECT COALESCE(MIN(id), 0) FROM {src};"

ROWCOUNT = "SELECT COUNT(*) FROM {tbl};"

HASH_AGG = r"""
-- Hash of all rows (order independent). Adjust for larger memory if needed.
WITH chunks AS (
  SELECT md5(COALESCE(id::text,'') || '|' || COALESCE(name,'') || '|' || COALESCE(email,'')) AS h
  FROM {tbl}
)
SELECT md5(string_agg(h, ',' ORDER BY h)) FROM chunks;
"""

GRANTS_QUERY = r"""
SELECT grantee, privilege_type, is_grantable
FROM information_schema.role_table_grants
WHERE table_schema = %s AND table_name = %s;
"""

REFERENCING_FKS = r"""
SELECT
    tc.constraint_name,
    tc.table_schema,
    tc.table_name
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
  ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage AS ccu
  ON ccu.constraint_name = tc.constraint_name
WHERE constraint_type = 'FOREIGN KEY'
  AND ccu.table_schema = %s
  AND ccu.table_name = %s;
"""

# -------------- Core --------------

def detect_referencing_fks(cur, src_qualified: str) -> List[Tuple[str, str, str]]:
    schema, name = src_qualified.split(".")
    rows = fetchall(cur, REFERENCING_FKS, (schema, name))
    return rows

def replay_grants(cur, from_rel: str, to_rel: str):
    schema, name = from_rel.split(".")
    grants = fetchall(cur, GRANTS_QUERY, (schema, name))
    for grantee, privilege_type, is_grantable in grants:
        # Skip the owner (grants not needed) and PUBLIC special if already implicit
        if grantee is None:
            continue
        grantable = " WITH GRANT OPTION" if is_grantable == "YES" else ""
        sql = f'GRANT {privilege_type} ON {to_rel} TO "{grantee}"{grantable};'
        cur.execute(sql)

def prepare(conn, src="public.customers", dst="public.customers_new", partitions=8, range_size=None, explicit_ranges: Optional[List[Tuple[int,int]]]=None, dry_run=False):
    with conn.cursor() as cur:
        # Check for FKs referencing customers
        fks = detect_referencing_fks(cur, src)
        if fks:
            print("WARNING: Foreign keys referencing source table detected:")
            for c, s, t in fks:
                print(f"  - {s}.{t} -> {c}")
            print("Cutover may require additional coordination. Use --force to proceed despite this.")
            if not args.force:
                raise RuntimeError("Aborting due to referencing foreign keys (use --force to override).")

        # Get id bounds to compute ranges if needed
        min_id = fetchone(cur, CHECK_MIN_ID.format(src=src))[0]
        max_id = fetchone(cur, CHECK_MAX_ID.format(src=src))[0]

        print(f"Detected id range in {src}: [{min_id}, {max_id}]")

        # Create parent partitioned table
        print(f"Creating parent partitioned table {dst}...")
        if not dry_run:
            execute(cur, CREATE_PARTITIONED_PARENT.format(dst=dst))

        # Build partition ranges
        ranges: List[Tuple[int, Optional[int]]] = []
        if explicit_ranges:
            for lo, hi in explicit_ranges:
                ranges.append((lo, hi))
            # Add MAXVALUE partition if needed
            max_lo = max([r[1] for r in explicit_ranges if r[1] is not None] or [0])
            ranges.append((max_lo, None))
        else:
            if range_size is None:
                # derive evenly from [1 .. max_id] or default span if empty
                if max_id <= 0:
                    # create open-ended single partition starting at 1
                    ranges = [(1, None)]
                else:
                    # Ensure partitions count
                    span = max(1, max_id - min(1, min_id) + 1)
                    part_size = max(1, math.ceil(span / partitions))
                    start = min(1, min_id if min_id > 0 else 1)
                    current = start
                    for i in range(partitions):
                        nxt = current + part_size
                        ranges.append((current, nxt))
                        current = nxt
                    # add MAXVALUE catch-all
                    ranges.append((current, None))
            else:
                start = min(1, min_id if min_id > 0 else 1)
                current = start
                for i in range(partitions):
                    nxt = current + range_size
                    ranges.append((current, nxt))
                    current = nxt
                ranges.append((current, None))

        print("Planned partitions:")
        for lo, hi in ranges:
            if hi is None:
                print(f"  FROM {lo} TO MAXVALUE")
            else:
                print(f"  FROM {lo} TO {hi}")

        # Create child partitions
        if not dry_run:
            for lo, hi in ranges:
                if hi is None:
                    execute(cur, CREATE_CHILD_MAXPART.format(dst=dst, from_id=lo))
                else:
                    execute(cur, CREATE_CHILD_PARTITION.format(dst=dst, from_id=lo, to_id=hi))

        # Create dual-write trigger functions and triggers
        print("Creating dual-write triggers on source...")
        if not dry_run:
            execute(cur, TRIGGER_FN.format(src=src, dst=dst))
            execute(cur, TRIGGERS.format(src=src, dst=dst))

        # Ensure indexes on partitions (the PK is inherited via parent)
        print("Ensured parent PK; create additional indexes on partitions if needed (not implemented here).")

        conn.commit()
        print("Prepare: completed.")

def backfill(conn, src="public.customers", dst="public.customers_new", batch_size=100000, sleep=0.0, dry_run=False):
    with conn.cursor(name="bf_cursor", cursor_factory=psycopg2.extras.DictCursor) as cur:
        # Stream ids in order to avoid long locks and reduce memory.
        cur.itersize = batch_size
        cur.execute(f"SELECT id, name, email FROM {src} ORDER BY id;")
        total = 0
        batch = []
        def flush_batch(c, b):
            if not b:
                return 0
            args = ",".join(["%s"] * len(b))
            sql = f"INSERT INTO {dst} (id, name, email) VALUES {args} ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name, email=EXCLUDED.email;"
            if not dry_run:
                c2 = conn.cursor()
                c2.execute(sql, b)
                conn.commit()
                c2.close()
            return len(b)

        for row in cur:
            batch.append( (row["id"], row["name"], row["email"]) )
            if len(batch) >= batch_size:
                total += flush_batch(cur, batch)
                batch = []
                if sleep:
                    time.sleep(sleep)
        if batch:
            total += flush_batch(cur, batch)
        print(f"Backfill copied/upserted approx rows: {total}")

def validate(conn, src="public.customers", dst="public.customers_new"):
    with conn.cursor() as cur:
        src_count = fetchone(cur, ROWCOUNT.format(tbl=src))[0]
        dst_count = fetchone(cur, ROWCOUNT.format(tbl=dst))[0]
        print(f"Row counts -> source: {src_count}, dest: {dst_count}")

        src_hash = fetchone(cur, HASH_AGG.format(tbl=src))[0]
        dst_hash = fetchone(cur, HASH_AGG.format(tbl=dst))[0]
        print(f"Checksum -> source: {src_hash}, dest: {dst_hash}")

        ok = (src_hash == dst_hash) and (src_count == dst_count)
        if not ok:
            raise RuntimeError("Validation FAILED: counts or checksums differ.")
        print("Validation PASSED.")

def cutover(conn, src="public.customers", dst="public.customers_new", force=False):
    with conn.cursor() as cur:
        # Lock both tables to get a clean swap window.
        print("Acquiring locks and swapping tables...")
        # Save grants from old 'customers'
        schema, name = src.split(".")
        old_grants = fetchall(cur, GRANTS_QUERY, (schema, name))

        # Brief lock; perform swap
        # We keep dual-write triggers active up to the moment of swap.
        execute(cur, f"BEGIN;")
        execute(cur, f"LOCK TABLE {src} IN ACCESS EXCLUSIVE MODE;")
        execute(cur, f"LOCK TABLE {dst} IN ACCESS EXCLUSIVE MODE;")

        # Drop dual-write triggers (no more writes to old after rename)
        execute(cur, DROP_TRIGGERS.format(src=src))

        # Rename: customers -> customers_legacy, customers_new -> customers
        legacy = src + "_legacy"
        execute(cur, f"ALTER TABLE {src} RENAME TO {name}_legacy;")
        execute(cur, f"ALTER TABLE {dst} RENAME TO {name};")

        # Ensure the default sequence remains correct and owned by the new table
        # Re-attach default if needed (shares the same sequence name)
        execute(cur, f"ALTER TABLE {qualified(schema + '.' + name)} ALTER COLUMN id SET DEFAULT nextval('customers_id_seq');")
        # Ensure ownership (ignore errors if already set)
        execute(cur, f"ALTER SEQUENCE customers_id_seq OWNED BY {qualified(schema + '.' + name)}.id;")

        # Replay grants captured from original 'customers' to the new object
        for grantee, privilege_type, is_grantable in old_grants:
            grantable = " WITH GRANT OPTION" if is_grantable == "YES" else ""
            sql = f'GRANT {privilege_type} ON {qualified(schema + "." + name)} TO "{grantee}"{grantable};'
            execute(cur, sql)

        execute(cur, "COMMIT;")
        print("Cutover completed.")

def cleanup(conn, legacy="public.customers_legacy"):
    with conn.cursor() as cur:
        print("You may drop the legacy table after a safe retention period:")
        print(f"  DROP TABLE {legacy};")

def parse_ranges(s: str) -> List[Tuple[int,int]]:
    """
    Parse --ranges like: "1:1000000,1000000:2000000,2000000:MAX"
    Note: MAX will be handled by adding a MAXVALUE partition automatically.
    """
    out = []
    for part in s.split(","):
        lo, hi = part.split(":")
        lo = int(lo.strip())
        if hi.strip().upper() == "MAX":
            out.append((lo, None))
        else:
            out.append((lo, int(hi.strip())))
    return out

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Migrate customers table to range partitions by id with near-zero downtime.")
    ap.add_argument("--dsn", help="Postgres DSN. If omitted, uses PG* env vars.")
    ap.add_argument("--dry-run", action="store_true", help="Print what would happen without executing changes.")
    ap.add_argument("--run-all", action="store_true", help="Run prepare, backfill, validate, cutover sequentially.")
    ap.add_argument("--prepare", action="store_true", help="Create partitioned table, partitions, and dual-write triggers.")
    ap.add_argument("--backfill", action="store_true", help="Backfill data from source to destination in batches.")
    ap.add_argument("--validate", action="store_true", help="Validate that source and destination are in sync (counts + checksum).")
    ap.add_argument("--cutover", action="store_true", help="Swap tables with a brief lock.")
    ap.add_argument("--cleanup", action="store_true", help="Print drop command for legacy table.")
    ap.add_argument("--force", action="store_true", help="Proceed even if foreign keys reference the source table.")
    ap.add_argument("--partitions", type=int, default=8, help="How many partitions to create (ignored if --ranges provided).")
    ap.add_argument("--range-size", type=int, help="Fixed size for each partition range (ignored if --ranges provided).")
    ap.add_argument("--ranges", type=str, help='Explicit ranges, e.g. "1:1000000,1000000:2000000,2000000:MAX"')
    ap.add_argument("--batch-size", type=int, default=100000, help="Backfill batch size (rows).")
    ap.add_argument("--sleep", type=float, default=0.0, help="Sleep seconds between batches to reduce load.")
    args = ap.parse_args()

    dsn = dsn_from_env(args)
    conn = psycopg2.connect(dsn)
    conn.autocommit = False

    try:
        advisory_lock(conn)
        src = qualified("public.customers")
        dst = qualified("public.customers_new")

        if args.ranges:
            explicit_ranges = []
            for p in args.ranges.split(","):
                parts = p.split(":")
                lo = int(parts[0])
                hi = None if parts[1].strip().upper() == "MAX" else int(parts[1])
                explicit_ranges.append((lo, hi))
        else:
            explicit_ranges = None

        if args.run_all or args.prepare:
            prepare(conn, src=src, dst=dst, partitions=args.partitions, range_size=args.range_size, explicit_ranges=explicit_ranges, dry_run=args.dry_run)

        if args.run_all or args.backfill:
            backfill(conn, src=src, dst=dst, batch_size=args.batch_size, sleep=args.sleep, dry_run=args.dry_run)

        if args.run_all or args.validate:
            validate(conn, src=src, dst=dst)

        if args.run_all or args.cutover:
            cutover(conn, src=src, dst=dst, force=args.force)

        if args.cleanup:
            cleanup(conn, legacy="public.customers_legacy")

    finally:
        try:
            advisory_unlock(conn)
        except Exception:
            pass
        conn.close()
