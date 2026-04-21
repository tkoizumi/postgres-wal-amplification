from psycopg import sql

from metrics import METRICS


def build_query() -> str:
    select_list = ",\n    ".join(
        f"{metric.sql_expr} AS {metric.csv_name}" for metric in METRICS
    )

    return f"""
    SELECT
        {select_list}
    FROM pg_stat_wal w
    CROSS JOIN pg_stat_bgwriter bg
    CROSS JOIN (
        SELECT
            sum(tup_inserted) AS tup_inserted,
            sum(tup_updated) AS tup_updated,
            sum(tup_deleted) AS tup_deleted
        FROM pg_stat_database
        WHERE datname = current_database()
    ) db
    """
