from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class Metric:
    csv_name: str
    sql_expr: str
    parser: Callable[[Any], Any] = lambda x: x


METRICS = [
    Metric("ts", "now()"),
    Metric("wal_bytes", "w.wal_bytes", int),
    Metric("wal_records", "w.wal_records", int),
    Metric("wal_fpi", "w.wal_fpi", int),
    Metric("checkpoints_timed", "bg.checkpoints_timed", int),
    Metric("checkpoints_req", "bg.checkpoints_req", int),
    Metric("buffers_checkpoint", "bg.buffers_checkpoint", int),
    Metric("buffers_backend", "bg.buffers_backend", int),
    Metric("tup_inserted", "db.tup_inserted", int),
    Metric("tup_updated", "db.tup_updated", int),
    Metric("tup_deleted", "db.tup_deleted", int),
]
