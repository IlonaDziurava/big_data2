from cassandra.cluster import Cluster
import subprocess

cluster = Cluster(["cassandra-server"])
session = cluster.connect("search_engine")

result = subprocess.run(
    ["hdfs", "dfs", "-cat", "/tmp/new_doc_output/part-*"],
    capture_output=True,
    text=True,
    check=False,
)

if result.returncode != 0:
    print("Error reading MapReduce output:", result.stderr)
    raise SystemExit(1)

for line in result.stdout.strip().split("\n"):
    if not line:
        continue

    parts = line.split("\t")
    if len(parts) < 2:
        continue

    record_type = parts[0]

    if record_type == "VOCAB":
        term, inc = parts[1], int(parts[2])
        rows = list(session.execute("SELECT df FROM vocabulary WHERE term = %s", (term,)))
        if rows:
            new_df = int(rows[0].df) + inc
            session.execute("UPDATE vocabulary SET df = %s WHERE term = %s", (new_df, term))
        else:
            session.execute(
                "INSERT INTO vocabulary (term, df) VALUES (%s, %s)", (term, inc)
            )

    elif record_type == "INDEX" and len(parts) >= 4:
        term, doc_id, tf = parts[1], parts[2], int(parts[3])
        session.execute(
            "INSERT INTO inverted_index (term, doc_id, tf) VALUES (%s, %s, %s)",
            (term, doc_id, tf),
        )

    elif record_type == "STATS" and len(parts) >= 5:
        doc_id, doc_title = parts[1], parts[2]
        doc_length, unique_terms = int(parts[3]), int(parts[4])
        session.execute(
            """
            INSERT INTO document_stats (doc_id, doc_title, doc_length, unique_terms)
            VALUES (%s, %s, %s, %s)
            """,
            (doc_id, doc_title, doc_length, unique_terms),
        )

cluster.shutdown()
print("Cassandra tables updated!")
