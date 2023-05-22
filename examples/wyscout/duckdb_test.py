import time

import duckdb
import pandas as pd


def main():
    df = pd.DataFrame([
        {"test": 1}
    ])
    df.to_parquet("/tmp/blaat.parquet")

    con = duckdb.connect()
    con.execute("CREATE TABLE files(name VARCHAR(250))")
    con.execute("INSERT INTO files(name) VALUES('/tmp/crap.parquet')")

    files = ["/tmp/blaat.parquet"] * 100_000

    t0 = time.time()
    con.execute("SELECT COUNT(*) FROM parquet_scan(?)", [files])
    t1 = time.time()
    print(con.fetchall())
    took = t1 - t0
    print(f"Took: {took * 1000:.1f}ms")


if __name__ == "__main__":
    main()
