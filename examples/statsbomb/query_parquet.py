import logging
import sys

from kloppy.utils import performance_logging


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
    )
    import duckdb

    con = duckdb.connect()
    with performance_logging("query parquet"):
        res = con.query(
            """
        SELECT 
            match, 
            COUNT(*) 
        FROM 
            'all_new.parquet'
        GROUP BY 
            match 
        ORDER BY 
            COUNT(*) DESC 
        LIMIT 10"""
        )
        print(res)


if __name__ == "__main__":
    main()
