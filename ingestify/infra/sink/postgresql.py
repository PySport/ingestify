from io import StringIO

import pandas as pd
from sqlalchemy import create_engine, text

from ingestify.domain.models import Dataset, Sink


# https://stackoverflow.com/questions/13947327/to-ignore-duplicate-keys-during-copy-from-in-postgresql
def _copy_to(conn, tablename, data, extra_columns):
    if isinstance(data, pd.DataFrame):
        for k, v in extra_columns:
            data[k] = v
        tsv_file = data.to_csv(sep="\t", header=True, index=False)
        stream = StringIO(tsv_file)
    else:
        raise Exception("Dont know how to handle data")

    raw_connection = conn.connection
    driver = conn.engine.dialect.driver

    sql = f"COPY {tablename} FROM STDIN WITH CSV DELIMITER '\t' HEADER"
    if driver == "pg8000":
        # https://github.com/tlocke/pg8000/blob/13bc039e805e8a2cd8d816b939362b40018ea8ef/test/native/test_copy.py
        raw_connection.run(sql=sql, stream=stream)
    elif driver == "pgcopy2":
        # https://github.com/psycopg/psycopg2/blob/1d3a89a0bba621dc1cc9b32db6d241bd2da85ad1/tests/test_copy.py
        with raw_connection.cursor() as cursor:
            cursor.copy_expert(sql=sql, file=stream)


class PostgresSQLSink(Sink):
    def __init__(self, url: str):
        self.engine = create_engine(url)

    def upsert(self, dataset: Dataset, data, params: dict):
        if not isinstance(data, pd.DataFrame):
            raise TypeError(
                f"Data {type(data)} is not supported by the PostgresSQLSink"
            )

        table_name = params["table_name"]

        with self.engine.begin() as conn:
            conn.query(
                text(
                    f"DELETE FROM {table_name} WHERE dataset_id = {dataset.dataset_id}"
                )
            )
            _copy_to(conn, table_name, data, dict(dataset_id=dataset.dataset_id))
