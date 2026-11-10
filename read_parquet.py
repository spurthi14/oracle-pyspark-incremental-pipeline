from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder
         .master("local[*]")
         .appName("read_parquet")
         .getOrCreate())
df=spark.read.parquet("output/customers")
df = df.withColumn("CUSTOMER_ID",F.col("customer_id").cast("int"))
df = df.withColumn("LOAD_DATE",F.create_date())
df.count()
df.show()