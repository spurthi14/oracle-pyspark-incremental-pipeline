from pyspark.sql import SparkSession
spark = (SparkSession.builder
         .master("local[*]")
         .appName("check_spark")
         .getOrCreate())

print("Spark version:", spark.version)

spark.stop()