from pyspark.sql import SparkSession
import os
pwd = os.environ.get("ORA_PAS")
spark = (SparkSession.builder
         .master("local[*]")
         .appName("read_oracle")
         .config("spark.driver.extraClassPath", r"C:\Users\MYPC\Documents\ora-spark\jar\ojdbc17.jar")
         .getOrCreate())

df = (spark.read.format("jdbc")
      .option("url", "jdbc:oracle:thin:@//localhost:1521/XEPDB1")
      .option("dbtable", "customer")
      .option("user", "system")
      .option("password", pwd)
      .option("driver", "oracle.jdbc.OracleDriver")
      .load())

#df.show()
df.write.mode("overwrite").partitionBy("create_date").parquet("output/customers")
spark.stop()