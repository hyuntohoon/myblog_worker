#!/usr/bin/env python3
"""TEMPORARY (do not merge) — classify which shared_db V-migrations are applied
on the Neon TEST branch (TEST_DB_URL).

Read-only by construction: every migration file (own BEGIN/COMMIT stripped) is
executed inside ONE outer transaction that is always rolled back. Files run in
V-number order cumulatively, so a file that depends on an earlier missing one
sees its effects. Classification:

  - runs clean            -> MISSING (not applied on the branch)
  - duplicate-* SQLSTATE  -> applied
  - undefined-* SQLSTATE  -> applied(undef) — renames/drops already done;
                             needs human review of the printed code
  - anything else         -> ERROR — needs human review, treated as blocking

Output: one line per file + a final MISSING_LIST= line. No schema dump, no
credentials are ever printed.
"""
import os
import re
import sys
from pathlib import Path

import psycopg

MIG_DIR = Path(sys.argv[1])
URL = os.environ["TEST_DB_URL"].replace("postgresql+psycopg", "postgresql")

FILES = sorted(
    MIG_DIR.glob("V*__*.sql"),
    key=lambda p: int(re.match(r"V(\d+)__", p.name).group(1)),
)
STRIP = re.compile(r"^\s*(BEGIN|COMMIT);\s*$", re.M)
DUPLICATE = {"42P07", "42701", "42710", "42723", "42P04", "42P06"}
UNDEFINED = {"42P01", "42703"}

results = []
blocking = False
with psycopg.connect(URL, autocommit=False) as conn:
    with conn.cursor() as cur:
        for f in FILES:
            sql = STRIP.sub("", f.read_text())
            cur.execute("SAVEPOINT sp")
            try:
                cur.execute(sql)
                results.append((f.name, "MISSING", ""))
            except psycopg.Error as exc:
                code = exc.sqlstate or "?"
                cur.execute("ROLLBACK TO SAVEPOINT sp")
                if code in DUPLICATE:
                    results.append((f.name, "applied", code))
                elif code in UNDEFINED:
                    results.append((f.name, "applied(undef-review)", code))
                else:
                    results.append((f.name, "ERROR-review", code))
                    blocking = True
    conn.rollback()

width = max(len(name) for name, _, _ in results)
for name, status, code in results:
    print(f"{name:<{width}}  {status:<22} {code}")

print()
print("MISSING_LIST=" + " ".join(n for n, s, _ in results if s == "MISSING"))
sys.exit(1 if blocking else 0)
