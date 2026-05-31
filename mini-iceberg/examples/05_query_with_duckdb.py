"""Example 5: query the parquet files behind a mini-iceberg table from DuckDB.

This demonstrates that an external engine can read the data *through the
table format* — the engine asks the catalog for the live data files of the
current snapshot, then scans them as a unioned view.

DuckDB's native iceberg extension can't read our format (we're using JSON
manifests, not Avro). But the underlying Parquet is standard, so we can
hand DuckDB the file list and have it pretend to be an Iceberg reader.
"""
from __future__ import annotations

import os

import duckdb
from rich import print as rprint

from miniiceberg import Catalog


def main() -> None:
    warehouse = os.environ["ICEBERG_WAREHOUSE"]
    s3_options = {
        "key":           os.environ["ICEBERG_S3_KEY"],
        "secret":        os.environ["ICEBERG_S3_SECRET"],
        "client_kwargs": {"endpoint_url": os.environ["ICEBERG_S3_ENDPOINT"]},
    }
    catalog = Catalog(warehouse, s3_options=s3_options)

    table = catalog.load_table("demo.events")

    # Get the current snapshot's data files (the catalog's job)
    files = [f.path for f in table.scan().files()]
    rprint(f"[bold]data files for current snapshot:[/bold] {len(files)}")

    # Configure DuckDB to read from MinIO
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_endpoint='{os.environ["ICEBERG_S3_ENDPOINT"].replace("http://", "")}';
        SET s3_access_key_id='{os.environ["ICEBERG_S3_KEY"]}';
        SET s3_secret_access_key='{os.environ["ICEBERG_S3_SECRET"]}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)

    # Build a UNION ALL view across all snapshot files. union_by_name handles
    # schema evolution — older files without newly-added columns are filled with NULL.
    file_list_sql = ", ".join(f"'{p}'" for p in files)
    con.execute(
        f"CREATE OR REPLACE VIEW events AS "
        f"SELECT * FROM read_parquet([{file_list_sql}], union_by_name=true)"
    )

    rprint("\n[bold]SELECT * FROM events:[/bold]")
    rprint(con.execute("SELECT * FROM events ORDER BY id").fetchall())

    rprint("\n[bold]aggregation: actions per user[/bold]")
    rprint(con.execute("""
        SELECT user, COUNT(*) AS n, COUNT(DISTINCT action) AS distinct_actions
        FROM events GROUP BY user ORDER BY n DESC
    """).fetchall())

    # Show the snapshot history from the table itself
    rprint("\n[bold]snapshot history (read from metadata.json):[/bold]")
    for s in table.history():
        rprint(f"  {s.snapshot_id}  op={s.operation}  parent={s.parent_id}  schema_id={s.schema_id}")


if __name__ == "__main__":
    main()
