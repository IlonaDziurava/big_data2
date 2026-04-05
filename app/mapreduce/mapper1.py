#!/usr/bin/env python3
import sys
import re


def tokenize(text):
    """Extract and filter terms from text."""
    terms = re.findall(r'\b[a-z0-9]+\b', text.lower())
    return [t for t in terms if len(t) > 1 and not t.isdigit()]


for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    try:
        # Input format: doc_id \t doc_title \t doc_text
        parts = line.split('\t', 2)
        if len(parts) != 3:
            continue

        doc_id, doc_title, doc_text = parts

        if not doc_text.strip():
            continue

        terms = tokenize(doc_text)
        if not terms:
            continue

        # Term frequencies for this document
        term_freq = {}
        for term in terms:
            term_freq[term] = term_freq.get(term, 0) + 1

        doc_length = len(terms)
        unique_terms = len(term_freq)

        # Emit inverted index entries: INDEX\tterm\tdoc_id\ttf
        for term, tf in term_freq.items():
            print(f"INDEX\t{term}\t{doc_id}\t{tf}")

        # Emit vocabulary entries (one per unique term per doc): VOCAB\tterm\t1
        for term in term_freq:
            print(f"VOCAB\t{term}\t1")

        # Emit document stats: STATS\tdoc_id\tdoc_title\tdoc_length\tunique_terms
        print(f"STATS\t{doc_id}\t{doc_title}\t{doc_length}\t{unique_terms}")

    except Exception as e:
        print(f"MAPPER ERROR: {e}", file=sys.stderr)
        continue