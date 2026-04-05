#!/usr/bin/env python3
"""
Builds the MapReduce input file at HDFS /input/data from documents
already uploaded to HDFS /data. Used when a.parquet is not available
and the sample documents committed to the repository are used instead.
"""
import re
import subprocess

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("build_input").getOrCreate()
sc = spark.sparkContext


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
    title = m.group(2).replace("_", " ")
    text = (content or "").strip()
    if not text:
        return None
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

count = lines_rdd.count()
spark.stop()
print(f"Done. Built MapReduce input from {count} documents in HDFS /data.")