#!/usr/bin/env python3
# BM25 search: Cassandra index + PySpark RDD scoring (assignment formula).
import math
import os
import re
import sys

from pyspark import SparkConf, SparkContext

# Assignment: k1=1, b=0.75; IDF = log(N/df)
K1 = 1.0
B = 0.75


def tokenize(text):
    terms = re.findall(r"\b[a-z0-9]+\b", text.lower())
    return [t for t in terms if len(t) > 1 and not t.isdigit()]


def read_query():
    # First priority: environment variable (set by search.sh)
    q = os.environ.get("SEARCH_QUERY", "").strip()
    if q:
        return q
    # Second priority: command-line arguments
    if len(sys.argv) > 1:
        return " ".join(sys.argv[1:]).strip()
    # Last resort: stdin (only if data is actually available, with timeout guard)
    try:
        import select
        if select.select([sys.stdin], [], [], 2)[0]:
            q = sys.stdin.read().strip()
            if q:
                return q
    except Exception:
        pass
    return ""


def main():
    query_text = read_query()
    if not query_text:
        print("ERROR: No query provided. Set SEARCH_QUERY env var or pass as argument.", file=sys.stderr)
        sys.exit(1)

    print(f"Processing query: {query_text}", file=sys.stderr)

    conf = SparkConf().setAppName("BM25_Query")
    sc = SparkContext(conf=conf)

    try:
        from cassandra.cluster import Cluster

        query_terms = tokenize(query_text)
        if not query_terms:
            print("No valid query terms after tokenization.")
            return

        print(f"Query terms: {query_terms}", file=sys.stderr)

        cluster_cs = Cluster(["cassandra-server"])
        session = cluster_cs.connect("search_engine")

        # Fetch document stats
        stats_rows = list(
            session.execute(
                "SELECT doc_id, doc_title, doc_length FROM document_stats"
            )
        )
        if not stats_rows:
            print("Index is empty (no rows in document_stats).")
            cluster_cs.shutdown()
            return

        n_docs = len(stats_rows)
        total_len = sum(int(r.doc_length) for r in stats_rows)
        avg_doc_length = total_len / n_docs if n_docs else 1.0
        doc_stats = {
            r.doc_id: {"title": r.doc_title, "length": int(r.doc_length)}
            for r in stats_rows
        }

        print(f"Corpus: {n_docs} docs, avg_len={avg_doc_length:.1f}", file=sys.stderr)

        # Fetch IDF data for query terms
        term_data = {}
        for term in set(query_terms):
            rows = list(
                session.execute(
                    "SELECT term, df FROM vocabulary WHERE term = %s", (term,)
                )
            )
            if not rows or rows[0].df <= 0:
                continue
            df = rows[0].df
            term_data[term] = {"df": df, "idf": math.log(n_docs / df)}

        if not term_data:
            print("None of the query terms were found in the vocabulary.")
            cluster_cs.shutdown()
            return

        print(f"Matched terms: {list(term_data.keys())}", file=sys.stderr)

        # Fetch postings for matched terms
        index_entries = []
        for term in term_data:
            for row in session.execute(
                "SELECT doc_id, tf FROM inverted_index WHERE term = %s", (term,)
            ):
                index_entries.append((row.doc_id, term, int(row.tf)))

        cluster_cs.shutdown()

        if not index_entries:
            print("No index entries found for query terms.")
            return

        print(f"Total postings fetched: {len(index_entries)}", file=sys.stderr)

        # Broadcast lookup tables
        term_bc = sc.broadcast(term_data)
        doc_bc = sc.broadcast(doc_stats)
        avg_bc = sc.broadcast(avg_doc_length)

        def score_one(entry):
            doc_id, term, tf = entry
            tinfo = term_bc.value.get(term)
            dinfo = doc_bc.value.get(doc_id)
            if not tinfo or not dinfo:
                return (doc_id, 0.0)
            idf = tinfo["idf"]
            dl = dinfo["length"]
            avg_len = max(avg_bc.value, 1.0)
            num = tf * (K1 + 1)
            den = tf + K1 * (1 - B + B * (dl / avg_len))
            return (doc_id, idf * (num / den))

        rdd = sc.parallelize(index_entries)
        top = (
            rdd.map(score_one)
            .reduceByKey(lambda a, b: a + b)
            .takeOrdered(10, key=lambda x: -x[1])
        )

        print("\nTop 10 results (doc_id, title):")
        for rank, (doc_id, score) in enumerate(top, 1):
            info = doc_bc.value.get(doc_id, {})
            title = info.get("title", "")
            print(f"{rank}\t{doc_id}\t{title}")

    except Exception as e:
        print(f"ERROR in BM25 query: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)
    finally:
        try:
            sc.stop()
        except Exception:
            pass


if __name__ == "__main__":
    main()