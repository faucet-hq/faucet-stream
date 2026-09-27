# Regenerates people.orc with the reference (Apache ORC C++) writer bundled in pyarrow.
import datetime, decimal
import pyarrow as pa
import pyarrow.orc as orc

table = pa.table({
    "id": pa.array([1, 2, 3], pa.int64()),
    "name": pa.array(["ada", "linus", "grace"]),
    "score": pa.array([1.5, None, 3.25], pa.float64()),
    "day": pa.array([datetime.date(2024, 2, 29), datetime.date(1970, 1, 1), None], pa.date32()),
    "amount": pa.array([decimal.Decimal("12.30"), decimal.Decimal("-0.05"), None], pa.decimal128(10, 2)),
    "tags": pa.array([["a", "b"], [], None], pa.list_(pa.string())),
})
orc.write_table(table, "people.orc", compression="zstd")
