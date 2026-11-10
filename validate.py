from pyspark.sql import SparkSession
import os
pwd = os.environ.get("ORA_PAS")
spark = (SparkSession.builder.master("local[*]").appName("validate").config("spark.driver.extraClassPath",r"C:\Users\MYPC\Documents\ora-spark\jar\ojdbc17.jar").getOrCreate())
df = (spark.read.format("jdbc")
     .option("url", "jdbc:oracle:thin:@//localhost:1521/XEPDB1")
      .option("dbtable", "customer")
      .option("user", "system")
      .option("password", pwd)
      .option("driver", "oracle.jdbc.OracleDriver")
      .load())
df_cnt = df.count()
out_Df = spark.read.parquet("output/customers")
out_cnt = out_Df.count()
if(df_cnt == out_cnt):
    print("MATCH")
else :
    print("NOT MATCH")
    print("Oracle:", df_cnt, "Parquet:", out_cnt)
dups = out_Df.groupBy("CUSTOMER_ID").count().filter("count > 1")
print("Duplicate IDs:", dups.count())
spark.stop()
