from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from clickhouse_driver import Client
from datetime import datetime
import logging

POSTGRES_CONN_ID = "my_postgres"
CLICKHOUSE_HOST = "clickhouse"


def load_book_rating_summary() -> None:
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    sql = """
        SELECT 
            b.book_id,
            b.title,
            b.authors,
            b.original_publication_year,
            r.rating
        FROM books b
        JOIN ratings r ON b.book_id = r.book_id;
    """
    df = pg_hook.get_pandas_df(sql)

    df["book_id"] = df["book_id"].astype("int32")
    df["original_publication_year"] = (
        df["original_publication_year"].fillna(0).astype("int32")
    )
    df["rating"] = df["rating"].astype("float32")
    df["title"] = df["title"].astype(str)
    df["authors"] = df["authors"].astype(str)

    summary_df = (
        df.groupby(["book_id", "title", "authors", "original_publication_year"])
        .agg(avg_rating=("rating", "mean"), num_ratings=("rating", "count"))
        .reset_index()
    )

    summary_df["avg_rating"] = summary_df["avg_rating"].astype("float32")
    summary_df["num_ratings"] = summary_df["num_ratings"].astype("int32")

    client = Client(host=CLICKHOUSE_HOST)
    data = list(summary_df.itertuples(index=False, name=None))
    if data:
        client.execute(
            "INSERT INTO books (book_id, title, authors, original_publication_year, avg_rating, num_ratings) VALUES",
            data,
            types_check=True,
        )

    logging.info("Inserted %d rows into ClickHouse summary table", len(data))


with DAG(
    "postgres_to_clickhouse",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["etl", "aggregation"],
) as dag:
    load_summary = PythonOperator(
        task_id="load",
        python_callable=load_book_rating_summary,
    )
