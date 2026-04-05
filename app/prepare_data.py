import glob
import os
import re
import shutil
import subprocess

from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

N_DOCS = 1000

spark = SparkSession.builder.appName("data preparation").getOrCreate()
sc = spark.sparkContext

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
    df = nonempty.orderBy(F.rand(seed=0)).limit(N_DOCS)
else:
    df = nonempty
    print(f"WARNING: only {total} non-empty rows (assignment asks for >= 1000).")

# Real titles for MapReduce lines: id -> title (tabs stripped)
id_to_title = {
    str(r["id"]): str(r["title"] or "").replace("\t", " ").strip()
    for r in df.select("id", "title").distinct().collect()
}
titles_bc = sc.broadcast(id_to_title)

if os.path.isdir("data"):
    shutil.rmtree("data")
os.makedirs("data", exist_ok=True)

written = 0
for row in df.collect():
    text = row["text"]
    if not text or not str(text).strip():
        continue
    filename = sanitize_filename(f"{row['id']}_{row['title']}").replace(" ", "_")
    path = f"data/{filename}.txt"
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    written += 1

print(f"Wrote {written} local .txt files under data/")

subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", "/data"], check=False)
subprocess.run(["hdfs", "dfs", "-mkdir", "-p", "/data"], check=True)
for p in glob.glob("data/*.txt"):
    subprocess.run(["hdfs", "dfs", "-put", "-f", p, "/data/"], check=True)


def parse_doc(pair):
    uri, content = pair
    name = uri.rsplit("/", 1)[-1]
    if not name.endswith(".txt"):
        return None
    name = name[:-4]
    m = re.match(r"^([^_]+)_(.+)$", name)
    if not m:
        return None
    doc_id, _fn_title = m.group(1), m.group(2)
    title = titles_bc.value.get(doc_id)
    if not title:
        title = _fn_title.replace("_", " ")
    text = (content or "").strip()
    if not text:
        return None
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
print("Prepared hdfs:///data and hdfs:///input/data (1 partition).")
