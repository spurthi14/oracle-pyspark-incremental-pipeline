from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
pwd =os.environ.get("ORA_PAS")
spark = (SparkSession.builder.master("local[*]").appName("incremental_load").config("spark.driver.extraClassPath", r"C:\Users\MYPC\Documents\ora-spark\jar\ojdbc17.jar").getOrCreate())
df =spark.read.parquet("output/customers")
max_Date = df.agg(F.max(F.col("create_Date"))).collect()[0][0]
wm = max_Date.strftime("%Y-%m-%d %H:%M:%S")
qry = f"(select * from customer where create_Date> TIMESTAMP '{wm}') t"
print(qry)

new_df = (spark.read.format("jdbc")
      .option("url", "jdbc:oracle:thin:@//localhost:1521/XEPDB1")
      .option("dbtable", qry)
      .option("user", "system")
      .option("password", pwd)
      .option("driver", "oracle.jdbc.OracleDriver")
      .load())
new_df.write.mode("append").partitionBy("create_date").parquet("output/customers")
new_df.show()
inc_df = spark.read.parquet("output/customers")
inc_df.count()
inc_df.printSchema()
inc_df.show()
