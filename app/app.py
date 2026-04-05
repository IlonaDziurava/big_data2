#!/usr/bin/env python3
"""
Load inverted index, vocabulary, and document stats from HDFS (/indexer/*)
into Cassandra (cassandra-driver). Called by store_index.sh.
"""
import subprocess
import sys

from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement


def truncate_tables(session):
    for tbl in ("inverted_index", "vocabulary", "document_stats"):
        session.execute(SimpleStatement(f"TRUNCATE {tbl}"))
    print("Tables truncated for reload")


def load_vocabulary(session):
    print("Loading vocabulary...")
    result = subprocess.run(
        ["hdfs", "dfs", "-cat", "/indexer/vocabulary/data.txt"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        print("Error reading vocabulary:", result.stderr)
        return
    count = 0
    for line in result.stdout.strip().split("\n"):
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) != 2:
            continue
        term, df_s = parts
        session.execute(
            "INSERT INTO vocabulary (term, df) VALUES (%s, %s)", (term, int(df_s))
        )
        count += 1
        if count % 5000 == 0:
            print(f"  ... {count} terms")
    print(f"Loaded {count} vocabulary terms")


def load_inverted_index(session):
    print("Loading inverted index...")
    result = subprocess.run(
        ["hdfs", "dfs", "-cat", "/indexer/index/data.txt"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        print("Error reading index:", result.stderr)
        return
    count = 0
    for line in result.stdout.strip().split("\n"):
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) != 3:
            continue
        term, doc_id, tf_s = parts
        session.execute(
            "INSERT INTO inverted_index (term, doc_id, tf) VALUES (%s, %s, %s)",
            (term, doc_id, int(tf_s)),
        )
        count += 1
        if count % 5000 == 0:
            print(f"  ... {count} postings")
    print(f"Loaded {count} index entries")


def load_document_stats(session):
    print("Loading document stats...")
    result = subprocess.run(
        ["hdfs", "dfs", "-cat", "/indexer/stats/data.txt"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        print("Error reading stats:", result.stderr)
        return
    count = 0
    for line in result.stdout.strip().split("\n"):
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) != 4:
            continue
        doc_id, doc_title, dl_s, ut_s = parts
        session.execute(
            """
            INSERT INTO document_stats (doc_id, doc_title, doc_length, unique_terms)
            VALUES (%s, %s, %s, %s)
            """,
            (doc_id, doc_title, int(dl_s), int(ut_s)),
        )
        count += 1
        if count % 500 == 0:
            print(f"  ... {count} docs")
    print(f"Loaded {count} document stats rows")


def main():
    print("Connecting to Cassandra...")
    cluster = Cluster(["cassandra-server"])
    session = cluster.connect("search_engine")
    print("Connected.")

    try:
        truncate_tables(session)
        load_vocabulary(session)
        load_inverted_index(session)
        load_document_stats(session)
        print("All data loaded successfully!")
    except Exception as e:
        print("Error loading data:", e)
        sys.exit(1)
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    main()
