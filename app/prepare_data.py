#!/usr/bin/env python3
"""
Reads a.parquet from HDFS, samples 1000 documents, writes them as .txt files
to local disk and HDFS /data, then creates /input/data (one partition) in
tab-separated format: doc_id \t doc_title \t doc_text
"""
import glob
import os
import re
import shutil
import subprocess

from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

N_DOCS = 1000

spark = SparkSession.builder.appName("data_preparation").getOrCreate()
sc = spark.sparkContext

# --- Load and filter ---
raw = spark.read.parquet("hdfs:///a.parquet").select("id", "title", "text")
nonempty = raw.filter(
    F.col("text").isNotNull() & (F.length(F.trim(F.col("text"))) > 0)
)

total = nonempty.count()
if total == 0:
    print("ERROR: No rows with non-empty text in hdfs:///a.parquet")
    spark.stop()
    raise SystemExit(1)

if total >= N_DOCS:
    df = nonempty.orderBy(F.rand(seed=42)).limit(N_DOCS)
else:
    df = nonempty
    print(f"WARNING: only {total} non-empty rows found (need >= {N_DOCS}).")

# Collect all rows to driver (we're in local mode so this is fine)
rows = df.collect()
print(f"Collected {len(rows)} rows.")

# --- Write local .txt files ---
if os.path.isdir("data"):
    shutil.rmtree("data")
os.makedirs("data", exist_ok=True)

written = 0
id_to_title = {}

for row in rows:
    doc_id = str(row["id"])
    title = str(row["title"] or "").replace("\t", " ").strip()
    text = str(row["text"] or "").strip()

    if not text:
        continue

    id_to_title[doc_id] = title

    safe_name = sanitize_filename(f"{doc_id}_{title}").replace(" ", "_")
    path = f"data/{safe_name}.txt"
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    written += 1

print(f"Wrote {written} local .txt files under data/")

# --- Upload to HDFS /data ---
subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", "/data"], check=False)
subprocess.run(["hdfs", "dfs", "-mkdir", "-p", "/data"], check=True)
for p in glob.glob("data/*.txt"):
    subprocess.run(["hdfs", "dfs", "-put", "-f", p, "/data/"], check=True)

print("Uploaded .txt files to HDFS /data")

# --- Build MapReduce input: one line per doc ---
titles_bc = sc.broadcast(id_to_title)


def parse_doc(pair):
    uri, content = pair
    name = uri.rsplit("/", 1)[-1]
    if not name.endswith(".txt"):
        return None
    name = name[:-4]
    m = re.match(r"^([^_]+)_(.+)$", name)
    if not m:
        return None
    doc_id = m.group(1)
    title = titles_bc.value.get(doc_id, m.group(2).replace("_", " "))
    text = (content or "").strip()
    if not text:
        return None
    # Tab-separated: doc_id \t doc_title \t doc_text (tabs stripped from content)
    return (
        doc_id,
        title.replace("\t", " "),
        text.replace("\t", " "),
    )


lines_rdd = (
    sc.wholeTextFiles("hdfs:///data")
    .map(parse_doc)
    .filter(lambda x: x is not None)
    .map(lambda t: "\t".join(t))
    .coalesce(1)
)

subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", "/input/data"], check=False)
subprocess.run(["hdfs", "dfs", "-mkdir", "-p", "/input"], check=True)
lines_rdd.saveAsTextFile("hdfs:///input/data")

spark.stop()
print("Done. Created hdfs:///data and hdfs:///input/data (1 partition).")