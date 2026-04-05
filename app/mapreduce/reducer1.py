#!/usr/bin/env python3
import sys

current_key = None
current_type = None
aggregated_data = []

def emit_result():
    """Emit aggregated results based on type"""
    global current_key, current_type, aggregated_data
    
    if not current_key or not current_type:
        return
    
    if current_type == "VOCAB":
        # For vocabulary: count document frequency
        df = len(aggregated_data)
        print(f"VOCAB\t{current_key}\t{df}")
    
    elif current_type == "INDEX":
        # For inverted index: emit all (doc_id, tf) pairs
        for doc_id, tf in aggregated_data:
            print(f"INDEX\t{current_key}\t{doc_id}\t{tf}")
    
    elif current_type == "STATS":
        # For document stats: emit single record
        if aggregated_data:
            doc_title, doc_length, unique_terms = aggregated_data[0]
            print(f"STATS\t{current_key}\t{doc_title}\t{doc_length}\t{unique_terms}")

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    
    try:
        parts = line.split('\t')
        record_type = parts[0]
        
        if record_type == "VOCAB":
            # VOCAB term 1
            term = parts[1]
            key = term
            
            if current_key == key and current_type == record_type:
                aggregated_data.append(1)
            else:
                emit_result()
                current_key = key
                current_type = record_type
                aggregated_data = [1]
        
        elif record_type == "INDEX":
            # INDEX term doc_id tf
            term = parts[1]
            doc_id = parts[2]
            tf = int(parts[3])
            key = term
            
            if current_key == key and current_type == record_type:
                aggregated_data.append((doc_id, tf))
            else:
                emit_result()
                current_key = key
                current_type = record_type
                aggregated_data = [(doc_id, tf)]
        
        elif record_type == "STATS":
            # STATS doc_id doc_title doc_length unique_terms
            doc_id = parts[1]
            doc_title = parts[2]
            doc_length = int(parts[3])
            unique_terms = int(parts[4])
            key = doc_id
            
            if current_key == key and current_type == record_type:
                pass  # Should only be one record per doc
            else:
                emit_result()
                current_key = key
                current_type = record_type
                aggregated_data = [(doc_title, doc_length, unique_terms)]
    
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        continue

# Don't forget the last group
emit_result()