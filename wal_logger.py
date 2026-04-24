import csv
import time
from pathlib import Path
from typing import cast

import psycopg
from psycopg.abc import Query

from builder import build_csv_headers, build_dsn, build_query, build_wal_bytes_query


def main():
    log_file_name = "wal_log.csv"
    dbname = "wal_test"
    user = "taka"
    host = "localhost"

    query = cast(Query, build_query())
    csv_headers = build_csv_headers()
    wal_bytes_query = cast(Query, build_wal_bytes_query())
    dsn = build_dsn(dbname, user, host)
    print(dsn)

    prev_wal_bytes = 0

    conn = psycopg.connect(dsn, autocommit=True)
    cur = conn.cursor()

    with open(log_file_name, "a+", newline="") as f:
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
            cur.execute(wal_bytes_query)
            row = cur.fetchone()
            if row == None:
                continue
            else:
                wal_bytes = int(row[0])
                if wal_bytes <= prev_wal_bytes:
                    continue
                else:
                    with open(log_file_name, "a", newline="") as f:
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
