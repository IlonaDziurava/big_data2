-- Create keyspace
CREATE KEYSPACE IF NOT EXISTS search_engine 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE search_engine;

-- Vocabulary table
CREATE TABLE IF NOT EXISTS vocabulary (
    term TEXT PRIMARY KEY,
    df INT
);

-- Inverted index table
CREATE TABLE IF NOT EXISTS inverted_index (
    term TEXT,
    doc_id TEXT,
    tf INT,
    PRIMARY KEY (term, doc_id)
);

-- Document stats table
CREATE TABLE IF NOT EXISTS document_stats (
    doc_id TEXT PRIMARY KEY,
    doc_title TEXT,
    doc_length INT,
    unique_terms INT
);