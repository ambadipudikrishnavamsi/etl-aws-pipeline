import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME", "RAW_PATH", "CURATED_PATH"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read raw JSON records
df = spark.read.json(args['RAW_PATH'])

# Example transform: select a few columns and rename
from pyspark.sql.functions import col
cols = [c for c in df.columns if c in ("userId","id","title","completed")]
df2 = df.select([col(c).alias(c.lower()) for c in cols])

# Write curated parquet with partitioning for performance
(df2.write
    .mode("overwrite")
    .partitionBy("userid")
    .parquet(args['CURATED_PATH']))

job.commit()
