from datetime import datetime
from sqlite3 import Timestamp
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

schema_online_retail = StructType([
	StructField("InvoiceNo", StringType(), True),
	StructField("StockCode", StringType(), True),
	StructField("Description", StringType(), True),
	StructField("Quantity", IntegerType(), True),
	StructField("InvoiceDate", TimestampType(), True),
	StructField("UnitPrice", FloatType(), True),
	StructField("CustomerID", IntegerType(), True),
	StructField("Country", StringType(), True)
])

 
if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Online Retail]"))

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          .schema(schema_online_retail)
		          .load("/home/spark/capgemini-aceleracao-pyspark/data/online-retail/online-retail.csv"))
	print(df.show())
