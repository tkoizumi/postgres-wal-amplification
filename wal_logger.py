import csv
import time
from pathlib import Path
from typing import cast

import psycopg
from psycopg.abc import Query

from query_builder import build_csv_headers, build_query

DSN = "dbname=wal_test user=taka host=localhost"


def main():
    log_file_name = "wal_log.csv"
    query = cast(Query, build_query())
    csv_headers = build_csv_headers()

    prev_wal_bytes = 0

    conn = psycopg.connect(DSN, autocommit=True)
    cur = conn.cursor()

    with open("wal_log.csv", "a+", newline="") as f:
        if not Path(log_file_name).exists():
            writer = csv.writer(f)
            writer.writerow(csv_headers)
        else:
            f.seek(0)
            reader = csv.reader(f)
            rows = list(reader)
            print(rows)
            last_row = rows[-1]
            prev_wal_bytes = int(last_row[1])

    try:

        while True:
            cur.execute("SELECT w.wal_bytes FROM pg_stat_wal w")
            row = cur.fetchone()
            if row == None:
                continue
            else:
                wal_bytes = int(row[0])
                if wal_bytes <= prev_wal_bytes:
                    continue
                else:
                    with open("wal_log.csv", "a", newline="") as f:
                        writer = csv.writer(f)
                        cur.execute(query)
                        row = cur.fetchone()
                        if row == None:
                            return
                        writer.writerow(row)
                        f.flush()
                        print(row)

                    prev_wal_bytes = wal_bytes

            time.sleep(1)
    finally:
        cur.close()


if __name__ == "__main__":
    main()
