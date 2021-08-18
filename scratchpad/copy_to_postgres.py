import time
from io import BytesIO
from os import environ

from dotenv import load_dotenv
from sqlalchemy import create_engine


def _copy_to(conn, table, data):
    stream = BytesIO(b"col1\tcol2\n" + (b"1\t1\n" * 50_000))

    raw_connection = conn.connection
    driver = conn.engine.dialect.driver

    sql = f"COPY {table} FROM STDIN WITH CSV DELIMITER '\t' HEADER"
    if driver == "pg8000":
        # https://github.com/tlocke/pg8000/blob/13bc039e805e8a2cd8d816b939362b40018ea8ef/test/native/test_copy.py
        raw_connection.run(sql=sql, stream=stream)
    elif driver == "pgcopy2":
        # https://github.com/psycopg/psycopg2/blob/1d3a89a0bba621dc1cc9b32db6d241bd2da85ad1/tests/test_copy.py
        with raw_connection.cursor() as cursor:
            cursor.copy_expert(sql=sql, file=stream)


# https://stackoverflow.com/questions/43317376/how-can-i-use-psycopg2-extras-in-sqlalchemy


def main():
    load_dotenv()

    engine = create_engine(environ["DATABASE_URL"] + "/360scouting")

    with engine.begin() as conn:
        start = time.time()
        _copy_to(conn, "statsbomb_events", [{"col1": 1, "col2": 10}])

        took = time.time() - start
        print(f"Took: {took * 1000:.2f}ms")


if __name__ == "__main__":
    main()
