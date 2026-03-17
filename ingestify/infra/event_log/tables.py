from sqlalchemy import BigInteger, Column, Integer, JSON, MetaData, String, Table

from ingestify.infra.store.dataset.sqlalchemy.tables import TZDateTime


def get_tables(table_prefix: str = ""):
    metadata = MetaData()

    event_log_table = Table(
        f"{table_prefix}event_log",
        metadata,
        Column(
            "id",
            Integer().with_variant(BigInteger(), "postgresql").with_variant(BigInteger(), "mysql"),
            primary_key=True,
            autoincrement=True,
        ),
        Column("event_type", String(255), nullable=False),
        Column("payload_json", JSON, nullable=False),
        Column("source", String(255)),
        Column("dataset_id", String(255)),
        Column("created_at", TZDateTime(6)),
    )

    reader_state_table = Table(
        f"{table_prefix}reader_state",
        metadata,
        Column("reader_name", String(255), primary_key=True),
        Column("last_event_id", BigInteger, nullable=False),
    )

    return {
        "metadata": metadata,
        "event_log_table": event_log_table,
        "reader_state_table": reader_state_table,
    }
