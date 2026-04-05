#!/usr/bin/env python3
"""
Reducer for the search engine indexer.

Hadoop streaming sorts all mapper output lines lexicographically before
sending to the reducer. The key structure is:

  INDEX\tterm\tdoc_id\ttf    -> sorts by "INDEX\tterm"
  VOCAB\tterm\t1             -> sorts by "VOCAB\tterm"
  STATS\tdoc_id\t...         -> sorts by "STATS\tdoc_id"

Because the first field (INDEX / STATS / VOCAB) is part of the sort key,
all records of each type cluster together, and within INDEX/VOCAB all
records for the same term cluster together.

Aggregation logic:
  VOCAB  term           -> count distinct docs (df)
  INDEX  term           -> emit (doc_id, tf) pairs unchanged
  STATS  doc_id         -> emit single stats record
"""
import sys

current_key = None
current_type = None
aggregated_data = []


def emit_result():
    global current_key, current_type, aggregated_data

    if not current_key or not current_type:
        return

    if current_type == "VOCAB":
        df = len(aggregated_data)
        print(f"VOCAB\t{current_key}\t{df}")

    elif current_type == "INDEX":
        for doc_id, tf in aggregated_data:
            print(f"INDEX\t{current_key}\t{doc_id}\t{tf}")

    elif current_type == "STATS":
        if aggregated_data:
            doc_title, doc_length, unique_terms = aggregated_data[0]
            print(f"STATS\t{current_key}\t{doc_title}\t{doc_length}\t{unique_terms}")


for line in sys.stdin:
    line = line.rstrip('\n')
    if not line:
        continue

    try:
        parts = line.split('\t')
        if len(parts) < 2:
            continue

        record_type = parts[0]

        if record_type == "VOCAB" and len(parts) >= 3:
            term = parts[1]
            key = term
            if current_key == key and current_type == "VOCAB":
                aggregated_data.append(1)
            else:
                emit_result()
                current_key = key
                current_type = "VOCAB"
                aggregated_data = [1]

        elif record_type == "INDEX" and len(parts) >= 4:
            term = parts[1]
            doc_id = parts[2]
            tf = int(parts[3])
            key = term
            if current_key == key and current_type == "INDEX":
                aggregated_data.append((doc_id, tf))
            else:
                emit_result()
                current_key = key
                current_type = "INDEX"
                aggregated_data = [(doc_id, tf)]

        elif record_type == "STATS" and len(parts) >= 5:
            doc_id = parts[1]
            doc_title = parts[2]
            doc_length = int(parts[3])
            unique_terms = int(parts[4])
            key = doc_id
            if current_key == key and current_type == "STATS":
                pass  # only one STATS record per doc_id
            else:
                emit_result()
                current_key = key
                current_type = "STATS"
                aggregated_data = [(doc_title, doc_length, unique_terms)]

    except Exception as e:
        print(f"REDUCER ERROR: {e} | line={line!r}", file=sys.stderr)
        continue

# Flush last group
emit_result()