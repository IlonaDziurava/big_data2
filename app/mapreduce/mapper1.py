#!/usr/bin/env python3
import sys
import re

def tokenize(text):
    """Extract terms from text"""
    # Convert to lowercase and split on non-alphanumeric
    terms = re.findall(r'\b[a-z0-9]+\b', text.lower())
    # Filter out single characters and numbers-only
    terms = [t for t in terms if len(t) > 1 and not t.isdigit()]
    return terms

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    
    try:
        # Parse tab-separated input: doc_id \t doc_title \t doc_text
        parts = line.split('\t', 2)
        if len(parts) != 3:
            continue
        
        doc_id, doc_title, doc_text = parts
        
        # Tokenize the text
        terms = tokenize(doc_text)
        
        if not terms:
            continue
        
        # Calculate term frequencies for this document
        term_freq = {}
        for term in terms:
            term_freq[term] = term_freq.get(term, 0) + 1
        
        # Emit for inverted index: term -> (doc_id, tf)
        for term, tf in term_freq.items():
            print(f"INDEX\t{term}\t{doc_id}\t{tf}")
        
        # Emit for vocabulary: term -> 1 (we'll count docs per term)
        for term in set(terms):
            print(f"VOCAB\t{term}\t1")
        
        # Emit for document stats: doc_id -> (title, length, unique_terms)
        doc_length = len(terms)
        unique_terms = len(term_freq)
        print(f"STATS\t{doc_id}\t{doc_title}\t{doc_length}\t{unique_terms}")
        
    except Exception as e:
        # Log errors to stderr
        print(f"ERROR: {e}", file=sys.stderr)
        continue