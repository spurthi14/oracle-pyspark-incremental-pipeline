from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import oracledb
passwd = os.environ.get("ORA_PAS")
spark = (SparkSession.builder.master("local[*]").appName("incremental_load").config("spark.driver.extraClassPath", r"C:\Users\MYPC\Documents\ora-spark\jar\ojdbc17.jar").getOrCreate())
max_dt = f"(select max(create_Date) as wm from customer) t "
new_df = (spark.read.format("jdbc")
      .option("url", "jdbc:oracle:thin:@//localhost:1521/XEPDB1")
      .option("dbtable", max_dt)
      .option("user", "system")
      .option("password", passwd)
      .option("driver", "oracle.jdbc.OracleDriver")
      .load())
wm = new_df.collect()[0][0]
if wm is None:
    wm = "1900-01-01 00:00:00"
else:
    wm = wm.strftime("%Y-%m-%d %H:%M:%S")
qry = f"(select * from customer_source where modified_Date> TIMESTAMP '{wm}') t"
print(qry)

new_df1 = (spark.read.format("jdbc")
      .option("url", "jdbc:oracle:thin:@//localhost:1521/XEPDB1")
      .option("dbtable", qry)
      .option("user", "system")
      .option("password", passwd)
      .option("driver", "oracle.jdbc.OracleDriver")
      .load())

con = oracledb.connect(user="system",password=passwd,dsn="localhost:1521/XEPDB1")
connect = con.cursor()
sql = """merge into customer s using customer_source t ON (s.customer_id=t.customer_id)
when matched then update  SET s.first_name=t.first_name,s.last_name=t.last_name,s.email=t.email,s.phone_number=t.phone_number,s.create_date=t.create_date
WHERE (s.first_name <> t.first_name OR s.last_name <> t.last_name OR s.email <> t.email OR s.phone_number <> t.phone_number)
when not matched then insert (customer_id,first_name,last_name,email,phone_number,create_date)  
values(t.customer_id,t.first_name,t.last_name,t.email,t.phone_number,t.create_date)"""
connect.execute(sql)
con.commit()
connect.close()
con.close()

