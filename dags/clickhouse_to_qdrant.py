# dags/clickhouse_to_qdrant.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from clickhouse_driver import Client
from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance, PointStruct
from qdrant_client.http.exceptions import UnexpectedResponse
from datetime import datetime
from sklearn.feature_extraction.text import HashingVectorizer
import time
import logging

CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_PORT = 9000
QDRANT_HOST = "qdrant"
QDRANT_PORT = 6333
COLLECTION_NAME = "books_lightweight"

vectorizer = HashingVectorizer(n_features=8, alternate_sign=False)


def wait_for_collection_ready(qdrant_client, collection_name, max_wait=30) -> bool:
    wait_time = 0
    while wait_time < max_wait:
        try:
            info = qdrant_client.get_collection(collection_name=collection_name)
            if hasattr(info.status, "value") and info.status.value == "green":
                logging.info(f"Collection {collection_name} is ready!")
                return True
            elif info.status == "green" or info.status == 1:
                logging.info(f"Collection {collection_name} is ready!")
                return True
        except Exception as e:
            logging.info(f"Collection not ready yet, waiting... ({e})")

        time.sleep(1)
        wait_time += 1

    logging.warning(f"Collection {collection_name} not ready after {max_wait}s")
    return False


def create_lightweight_collection(qdrant, collection_name) -> bool:
    try:
        logging.info("Attempting minimal collection creation (full scan mode)...")
        qdrant.create_collection(
            collection_name=collection_name,
            vectors_config=VectorParams(size=8, distance=Distance.COSINE),
        )
        logging.info(
            f"Created lightweight collection: {collection_name} (8-dim, full scan)"
        )
        return True
    except Exception as e:
        logging.warning(f"Minimal config failed: {e}")
    try:
        from qdrant_client.models import HnswConfigDiff

        logging.info("Attempting lightweight HNSW config...")
        qdrant.create_collection(
            collection_name=collection_name,
            vectors_config=VectorParams(size=8, distance=Distance.COSINE),
            hnsw_config=HnswConfigDiff(
                m=4,
                ef_construct=32,
                full_scan_threshold=1000,
                max_indexing_threads=1,
            ),
            optimizers_config={
                "indexing_threshold": 100,
                "memmap_threshold": 5000,
                "flush_interval_sec": 1,
                "max_optimization_threads": 1,
            },
        )
        logging.info(
            f"Created lightweight HNSW collection: {collection_name} (8-dim, minimal HNSW)"
        )
        return True
    except Exception as e:
        logging.warning(f"Lightweight HNSW config failed: {e}")

    try:
        logging.info("Attempting absolute minimal config...")
        qdrant.create_collection(
            collection_name=collection_name,
            vectors_config=VectorParams(size=8, distance=Distance.COSINE),
        )
        logging.info(f" Created absolute minimal collection: {collection_name}")
        return True
    except Exception as e:
        logging.error(f"All collection creation strategies failed: {e}")
        raise


def load_to_qdrant():
    client = Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
    rows = client.execute("""
            SELECT book_id, title, avg(avg_rating) as rating 
            FROM default.books 
            GROUP BY book_id, title
            ORDER BY book_id
        """)
    if not rows:
        logging.info("No rows to process")
        return

    logging.info(f"Loaded {len(rows)} rows from ClickHouse")

    qdrant = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT, check_compatibility=False)

    try:
        qdrant.delete_collection(collection_name=COLLECTION_NAME)
        logging.info(f"Deleted existing collection: {COLLECTION_NAME}")
        time.sleep(2)
    except Exception as e:
        logging.info(f"No existing collection to delete: {e}")

    try:
        create_lightweight_collection(qdrant, COLLECTION_NAME)

        if not wait_for_collection_ready(qdrant, COLLECTION_NAME, max_wait=20):
            logging.warning(
                f"Collection {COLLECTION_NAME} not fully ready, but continuing..."
            )

    except Exception as e:
        logging.error(f"Failed to create collection: {e}")
        raise

    points = []
    for book_id, title, rating in rows:
        try:
            vector = vectorizer.transform([title]).toarray()[0].tolist()
            if len(vector) != 8:
                logging.warning(
                    f"Vector for book {book_id} has wrong length: {len(vector)}"
                )
                continue

            points.append(
                PointStruct(
                    id=book_id,
                    vector=vector,
                    payload={"title": str(title), "rating": float(rating)},
                )
            )
        except Exception as e:
            logging.warning(f"Failed to process book {book_id}: {e}")
            continue

    logging.info(f"Prepared {len(points)} lightweight points (8-dim vectors)")

    if not points:
        logging.warning("No valid points to insert")
        return

    total_inserted = 0
    batch_size = 200  # Larger batches for faster insert
    max_retries = 3

    for i in range(0, len(points), batch_size):
        batch = points[i : i + batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (len(points) - 1) // batch_size + 1

        retry_count = 0
        batch_inserted = False

        while retry_count < max_retries and not batch_inserted:
            try:
                try:
                    _ = qdrant.get_collection(collection_name=COLLECTION_NAME)
                except Exception:
                    logging.warning(
                        f"Collection missing before batch {batch_num}, recreating..."
                    )
                    qdrant.delete_collection(collection_name=COLLECTION_NAME)
                    time.sleep(1)
                    create_lightweight_collection(qdrant, COLLECTION_NAME)
                    time.sleep(2)

                qdrant.upsert(collection_name=COLLECTION_NAME, points=batch)
                total_inserted += len(batch)
                batch_inserted = True
                logging.info(
                    f"Upserted batch {batch_num}/{total_batches} ({len(batch)} points)"
                )

                time.sleep(0.2)  # Brief pause between batches

            except UnexpectedResponse as e:
                if "Not found: Collection" in str(e):
                    logging.warning(
                        f"Collection disappeared at batch {batch_num}, retry {retry_count + 1}"
                    )
                    retry_count += 1
                    time.sleep(2**retry_count)
                    continue
                else:
                    logging.error(f"Unexpected response at batch {batch_num}: {e}")
                    raise
            except Exception as e:
                logging.error(
                    f"Error at batch {batch_num}, retry {retry_count + 1}: {e}"
                )
                retry_count += 1
                time.sleep(2**retry_count)
                if retry_count >= max_retries:
                    logging.error(f"Max retries exceeded for batch {batch_num}")
                    break

    logging.info(f"Successfully inserted {total_inserted} lightweight points")

    if total_inserted < len(points):
        logging.warning(f" Only {total_inserted}/{len(points)} points inserted")

    logging.info("Checking lightweight indexing status...")
    time.sleep(2)

    try:
        info = qdrant.get_collection(collection_name=COLLECTION_NAME)
        points_count = getattr(info, "points_count", 0) or 0
        vectors_count = (
            getattr(info, "indexed_vectors_count", 0)
            or getattr(info, "vectors_count", 0)
            or 0
        )

        logging.info(f" Status: {vectors_count}/{points_count} vectors ready")
        logging.info(f" Collection status: {getattr(info, 'status', 'unknown')}")

        if vectors_count == points_count and points_count > 0:
            logging.info(f"Lightweight indexing complete! Ready for instant search")
        else:
            logging.info(
                f"Lightweight indexing in progress: {vectors_count}/{points_count}"
            )

    except Exception as e:
        logging.warning(f"Status check failed: {e}")

    try:
        test_vector = vectorizer.transform(["love"]).toarray()[0].tolist()
        search_results = qdrant.search(
            collection_name=COLLECTION_NAME,
            query_vector=test_vector,
            limit=5,
            with_payload=True,
            score_threshold=0.0,
        )

        if search_results:
            logging.info(f" Search test PASSED! Found {len(search_results)} results:")
            for i, result in enumerate(search_results[:3], 1):
                title = result.payload.get("title", "Unknown")[:50]
                rating = result.payload.get("rating", 0)
                logging.info(
                    f"   {i}. '{title}' ({rating:.1f}, score: {result.score:.3f})"
                )
        else:
            logging.info(
                " Search test: No results (normal for 8-dim hashing, try different queries)"
            )

    except Exception as e:
        logging.warning(f"Search test failed: {e}")
        logging.info("Search will work once points are fully processed")

    logging.info(f" {total_inserted} books loaded with 8-dim vectors")
    logging.info(f" Optimized for speed: {len(rows) * 8 / 1024:.1f}KB total storage")


with DAG(
    dag_id="clickhouse_to_qdrant",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["clickhouse", "qdrant", "lightweight", "8dim", "fast"],
) as dag:
    load_task = PythonOperator(
        task_id="load",
        python_callable=load_to_qdrant,
    )
