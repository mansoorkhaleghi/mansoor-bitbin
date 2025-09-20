from clickhouse_driver import Client
from tabulate import tabulate

client = Client(host="localhost", port=9000)

# --- Query 1: Top 10 highest-rated books ---
# This query finds the books with the highest average rating,
# only considering books that have at least 50 ratings for reliability.
highest_rated_query = """
SELECT 
    title,
    authors,
    avg_rating,
    num_ratings
FROM books
WHERE num_ratings >= 50
ORDER BY avg_rating DESC
LIMIT 10;
"""

highest_rated_rows = client.execute(highest_rated_query)

print("\n=== Top 10 Highest-Rated Books (≥50 Ratings) ===")
print(
    tabulate(
        highest_rated_rows,
        headers=["Title", "Authors", "Avg Rating", "Num Ratings"],
        tablefmt="pretty",
    )
)

# --- Query 2: Most popular authors ---
# This query finds the authors whose books have the most total ratings across all their books.
# It also calculates the average rating across all books per author.
popular_authors_query = """
SELECT 
    authors,
    SUM(num_ratings) AS total_ratings,
    AVG(avg_rating) AS avg_rating_across_books
FROM books
GROUP BY authors
ORDER BY total_ratings DESC
LIMIT 10;
"""

popular_authors_rows = client.execute(popular_authors_query)

print("\n=== Top 10 Most Popular Authors (by Total Ratings) ===")
print(
    tabulate(
        popular_authors_rows,
        headers=["Authors", "Total Ratings", "Avg Rating Across Books"],
        tablefmt="pretty",
    )
)
