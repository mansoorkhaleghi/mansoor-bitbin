from qdrant_client import QdrantClient

COLL = "books_lightweight"

client = QdrantClient(host="localhost", port=6333)

total = client.count(collection_name=COLL, exact=False).count
print("Total points:", total)


points, next_page_offset = client.scroll(
    collection_name=COLL,
    limit=50,
    with_payload=True,
    with_vectors=True,
)

print(f"Fetched {len(points)} points. Next offset:", next_page_offset)
for p in points[:5]:
    print(p.id, p.payload, p.vector)
