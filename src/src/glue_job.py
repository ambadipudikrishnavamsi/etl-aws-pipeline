import sys
from pyspark.sql import functions as F
from pyspark.sql.types import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

# -------- Params expected from Glue Job arguments --------
# --RAW_PATH s3://<your-raw-bucket>/etl/raw/
# --CURATED_PATH s3://<your-curated-bucket>/etl/curated/
args = getResolvedOptions(sys.argv, ["JOB_NAME", "RAW_PATH", "CURATED_PATH"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

RAW_PATH = args["RAW_PATH"]
CURATED_PATH = args["CURATED_PATH"]

# -------- Read raw JSON (multi-file capable) --------
# The raw files are the ones written by Lambda from the placeholder API.
df_raw = spark.read.json(RAW_PATH)

# If the API shape changes, it's fine—select the columns we care about.
# jsonplaceholder "todos" example has: userId, id, title, completed
wanted_cols = ["userId", "id", "title", "completed"]
existing = [c for c in df_raw.columns if c in wanted_cols]
df = df_raw.select(*existing)

# Standardize column names to snake_case & types
rename_map = {
    "userId": "user_id",
    "id": "todo_id",
    "title": "title",
    "completed": "completed"
}
for old, new in rename_map.items():
    if old in df.columns:
        df = df.withColumnRenamed(old, new)

# Make types explicit & clean text
if "title" in df.columns:
    df = df.withColumn("title", F.trim(F.col("title").cast(StringType())))
if "user_id" in df.columns:
    df = df.withColumn("user_id", F.col("user_id").cast(IntegerType()))
if "todo_id" in df.columns:
    df = df.withColumn("todo_id", F.col("todo_id").cast(IntegerType()))
if "completed" in df.columns:
    # Handles true/false/strings/nulls robustly
    df = df.withColumn("completed", F.col("completed").cast(BooleanType()))

# Add metadata columns (useful for auditing/partitioning)
df = df.withColumn("ingest_date", F.current_date())           # yyyy-MM-dd
df = df.withColumn("ingest_ts_utc", F.current_timestamp())    # UTC timestamp

# -------- Write curated parquet (partitioned) --------
# Partitioning by user keeps files small & efficient for querying.
(
    df.write
      .mode("overwrite")                # for demos; change to "append" in prod
      .partitionBy("user_id")           # choose columns that have good cardinality
      .parquet(CURATED_PATH)
)

job.commit()
