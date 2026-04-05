#!/usr/bin/env python3
"""
Reads a.parquet from HDFS, takes N_DOCS documents, writes them as
<doc_id>_<doc_title>.txt to local disk and HDFS /data, then writes
the MapReduce input to HDFS /input/data (1 partition, TSV format).
"""
import glob
import os
import re
import shutil
import subprocess

from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

N_DOCS = 100

spark = SparkSession.builder.appName("data_preparation").getOrCreate()
sc = spark.sparkContext

# Load parquet
raw = spark.read.parquet("hdfs:///a.parquet").select("id", "title", "text")
nonempty = raw.filter(
    F.col("text").isNotNull() & (F.length(F.trim(F.col("text"))) > 0)
)
total = nonempty.count()
print(f"Total non-empty rows in parquet: {total}")

if total == 0:
    print("ERROR: no non-empty rows found in parquet!")
    spark.stop()
    raise SystemExit(1)

# Take up to N_DOCS rows
sample_df = nonempty.limit(N_DOCS)
rows = sample_df.collect()
print(f"Collected {len(rows)} rows for indexing.")

# Write local .txt files  
if os.path.isdir("data"):
    shutil.rmtree("data")
os.makedirs("data", exist_ok=True)

id_to_title = {}
written = 0

for row in rows:
    doc_id = str(row["id"])
    title = str(row["title"] or "").replace("\t", " ").strip()
    text = str(row["text"] or "").strip()
    if not text:
        continue
    id_to_title[doc_id] = title
    # Filename format: <doc_id>_<doc_title>.txt
    safe = sanitize_filename(f"{doc_id}_{title}").replace(" ", "_")
    # Truncate filename if too long
    if len(safe) > 200:
        safe = safe[:200]
    filepath = f"data/{safe}.txt"
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(text)
    written += 1

print(f"Written {written} .txt files to local data/")

if written == 0:
    print("ERROR: no files written!")
    spark.stop()
    raise SystemExit(1)

# Upload to HDFS /data
subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", "/data"], check=False)
subprocess.run(["hdfs", "dfs", "-mkdir", "-p", "/data"], check=True)
for p in glob.glob("data/*.txt"):
    result = subprocess.run(
        ["hdfs", "dfs", "-put", "-f", p, "/data/"],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"Warning: failed to upload {p}: {result.stderr}")
print(f"Uploaded files to HDFS /data")

# Verify upload
check = subprocess.run(
    ["hdfs", "dfs", "-count", "/data"],
    capture_output=True, text=True
)
print(f"HDFS /data count: {check.stdout.strip()}")

# Build MapReduce input: one line per doc, tab-separated
titles_bc = sc.broadcast(id_to_title)

def parse_doc(pair):
    uri, content = pair
    fname = uri.rsplit("/", 1)[-1]
    if not fname.endswith(".txt"):
        return None
    stem = fname[:-4]
    m = re.match(r"^([^_]+)_(.+)$", stem)
    if not m:
        return None
    doc_id = m.group(1)
    title = titles_bc.value.get(doc_id, m.group(2).replace("_", " "))
    text = (content or "").strip()
    if not text:
        return None
    # Output: doc_id \t doc_title \t doc_text  (tabs stripped from content)
    return "\t".join([
        doc_id,
        title.replace("\t", " "),
        text.replace("\t", " "),
    ])

lines_rdd = (
    sc.wholeTextFiles("hdfs:///data")
    .map(parse_doc)
    .filter(lambda x: x is not None)
    .coalesce(1)
)

subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", "/input/data"], check=False)
subprocess.run(["hdfs", "dfs", "-mkdir", "-p", "/input"], check=True)
lines_rdd.saveAsTextFile("hdfs:///input/data")

# Verify
count_result = subprocess.run(
    ["hdfs", "dfs", "-cat", "/input/data/part-00000"],
    capture_output=True, text=True
)
line_count = len([l for l in count_result.stdout.strip().split("\n") if l])
print(f"MapReduce input lines written: {line_count}")

spark.stop()
print(f"Done. {written} documents prepared in hdfs:///input/data")