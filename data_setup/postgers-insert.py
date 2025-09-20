import pandas as pd
from sqlalchemy import create_engine, text

engine = create_engine("postgresql+psycopg2://airflow:airflow@localhost:5432/airflow")

books_df = pd.read_csv("./data/books.csv")
ratings_df = pd.read_csv("./data/ratings.csv")

books_df = books_df[["book_id", "title", "authors", "original_publication_year"]]
ratings_df = ratings_df[["book_id", "user_id", "rating"]]

with engine.begin() as conn:
    conn.execute(
        text("""
        CREATE TABLE IF NOT EXISTS books (
            book_id INT PRIMARY KEY,
            title TEXT,
            authors TEXT,
            original_publication_year INT
        );
    """)
    )
    conn.execute(
        text("""
        CREATE TABLE IF NOT EXISTS ratings (
            rating_id SERIAL PRIMARY KEY,
            book_id INT REFERENCES books(book_id),
            user_id INT,
            rating INT
        );
    """)
    )

print("Inserting books...")
books_df.to_sql("books", engine, if_exists="append", index=False)

print("Filtering ratings to valid books...")
with engine.connect() as conn:
    valid_books = pd.read_sql("SELECT book_id FROM books", conn)

valid_book_ids = set(valid_books["book_id"])
ratings_df = ratings_df[ratings_df["book_id"].isin(valid_book_ids)]

print(f"Inserting {len(ratings_df)} ratings...")
ratings_df.to_sql("ratings", engine, if_exists="append", index=False)

print("Data successfully inserted into Postgres!")
