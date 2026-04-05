#!/usr/bin/env python3
"""
BM25 search engine query.
Runs Spark in local mode to avoid YARN memory issues in Docker.
All Cassandra access is in the driver process only.
"""
import math
import os
import re
import sys

from pyspark import SparkConf, SparkContext

K1 = 1.0
B = 0.75


def tokenize(text):
    terms = re.findall(r"\b[a-z0-9]+\b", text.lower())
    return [t for t in terms if len(t) > 1 and not t.isdigit()]


def read_query():
    # 1. env var (set by search.sh)
    q = os.environ.get("SEARCH_QUERY", "").strip()
    if q:
        return q
    # 2. command line args
    if len(sys.argv) > 1:
        return " ".join(sys.argv[1:]).strip()
    # 3. stdin with timeout
    try:
        import select
        if select.select([sys.stdin], [], [], 3)[0]:
            q = sys.stdin.read().strip()
            if q:
                return q
    except Exception:
        pass
    return ""


def main():
    query_text = read_query()
    if not query_text:
        print("ERROR: no query provided. Set SEARCH_QUERY env var.", file=sys.stderr)
        sys.exit(1)

    print(f"Query: '{query_text}'", file=sys.stderr)

    # Use local mode - reliable, no YARN needed, still uses Spark RDD API
    conf = (SparkConf()
            .setAppName("BM25_Query")
            .setMaster("local[*]"))
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")

    try:
        from cassandra.cluster import Cluster

        terms = tokenize(query_text)
        if not terms:
            print("No valid query terms after tokenisation.")
            return
        print(f"Terms: {terms}", file=sys.stderr)

        cs = Cluster(["cassandra-server"])
        s = cs.connect("search_engine")

        # Fetch all document stats
        stats_rows = list(s.execute(
            "SELECT doc_id, doc_title, doc_length FROM document_stats"
        ))
        if not stats_rows:
            print("Index is empty — run index.sh first.")
            cs.shutdown()
            return

        n_docs = len(stats_rows)
        avg_dl = sum(int(r.doc_length) for r in stats_rows) / n_docs
        doc_stats = {
            r.doc_id: {"title": r.doc_title, "length": int(r.doc_length)}
            for r in stats_rows
        }
        print(f"Corpus: {n_docs} docs, avg_length={avg_dl:.1f}", file=sys.stderr)

        # Fetch IDF for each query term
        term_data = {}
        for t in set(terms):
            rows = list(s.execute(
                "SELECT df FROM vocabulary WHERE term = %s", (t,)
            ))
            if rows and rows[0].df > 0:
                df = rows[0].df
                term_data[t] = {"df": df, "idf": math.log(n_docs / df)}

        if not term_data:
            print("No query terms found in the vocabulary.")
            cs.shutdown()
            return
        print(f"Matched terms: {list(term_data.keys())}", file=sys.stderr)

        # Fetch postings for matched terms
        postings = []
        for t in term_data:
            for row in s.execute(
                "SELECT doc_id, tf FROM inverted_index WHERE term = %s", (t,)
            ):
                postings.append((row.doc_id, t, int(row.tf)))

        cs.shutdown()

        if not postings:
            print("No postings found for query terms.")
            return
        print(f"Postings: {len(postings)}", file=sys.stderr)

        # Distribute to Spark workers via broadcast
        term_bc = sc.broadcast(term_data)
        doc_bc = sc.broadcast(doc_stats)
        avg_bc = sc.broadcast(avg_dl)

        def score_entry(entry):
            """Pure BM25 scoring - no imports needed on workers."""
            doc_id, term, tf = entry
            ti = term_bc.value.get(term)
            di = doc_bc.value.get(doc_id)
            if not ti or not di:
                return (doc_id, 0.0)
            idf = ti["idf"]
            dl = di["length"]
            avg = max(avg_bc.value, 1.0)
            num = tf * (K1 + 1)
            den = tf + K1 * (1 - B + B * (dl / avg))
            return (doc_id, idf * (num / den))

        # BM25 scoring with Spark RDD
        top10 = (
            sc.parallelize(postings)
            .map(score_entry)
            .reduceByKey(lambda a, b: a + b)
            .takeOrdered(10, key=lambda x: -x[1])
        )

        print("\nTop 10 results (doc_id, title):")
        for rank, (doc_id, score) in enumerate(top10, 1):
            info = doc_bc.value.get(doc_id, {})
            title = info.get("title", "")
            print(f"{rank}\t{doc_id}\t{title}")

    except Exception as exc:
        import traceback
        print(f"ERROR: {exc}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)
    finally:
        try:
            sc.stop()
        except Exception:
            pass


if __name__ == "__main__":
    main()