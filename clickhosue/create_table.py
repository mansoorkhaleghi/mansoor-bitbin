from clickhouse_driver import Client

client = Client(host="localhost", port=9000)

client.execute("""
	CREATE TABLE IF NOT EXISTS books (
		book_id Int32,
		title String,
		authors String,
		original_publication_year Int32,
		avg_rating Float32,
		num_ratings Int32
	)
	ENGINE = MergeTree()
	ORDER BY (book_id)
""")

client.execute("TRUNCATE TABLE books")
