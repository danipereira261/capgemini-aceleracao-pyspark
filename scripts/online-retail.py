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
	StructField("InvoiceDate", StringType(), True),
	StructField("UnitPrice", StringType(), True),
	StructField("CustomerID", IntegerType(), True),
	StructField("Country", StringType(), True)
])

def qa_InvoiceNo(df):
	df = df.withColumn("qa_InvoiceNo", 
	F.when(F.col("InvoiceNo").startswith("C"), "C")
	 .when(F.col("InvoiceNo").rlike("^[0-9]*$"), "OK").otherwise("F"))
	print(df.groupBy("qa_InvoiceNo").count().distinct().orderBy("qa_InvoiceNo", ascending=False).show())

def qa_StockCode(df):
	df = df.withColumn("qa_StockCode", 
	F.when(~F.col("StockCode").rlike("([0-9a-zA-Z]{5})"), "F")
	 .otherwise("OK"))
	print(df.groupBy("qa_StockCode").count().distinct().orderBy("qa_StockCode", ascending=False).show())

def qa_Description(df):
	df = df.withColumn("qa_Description", 
	F.when(F.col("Description").isNull(), "M")
	 .when(F.col("Description") == "", "M")
	 .otherwise("OK"))
	print(df.groupBy("qa_Description").count().distinct().orderBy("qa_Description", ascending=False).show())

def qa_Quantity(df):
	df = df.withColumn("qa_Quantity", 
	F.when(~F.col("Quantity").rlike("\d"), "N")
	 .otherwise("OK"))
	print(df.groupBy("qa_Quantity").count().distinct().orderBy("qa_Quantity", ascending=False).show())		

def qa_InvoiceDate(df):
	df = df.withColumn("InvoiceDate", 
	F.col("InvoiceDate").cast(TimestampType()))
	print(df.printSchema())

def qa_UnitPrice(df):
	df = df.withColumn("UnitPrice", 
	F.col("UnitPrice").cast(FloatType()))
	print(df.printSchema())

def qa_CustomerID(df):
	df = df.withColumn("qa_CustomerID", 
	F.when(~F.col("CustomerID").rlike("([0-9a-zA-Z]{5})"), "F").otherwise("OK"))
	print(df.groupBy("qa_CustomerID").count().distinct().orderBy("qa_CustomerID", ascending=False).show())

def qa_Country(df):
	df = df.withColumn("qa_Country", 
	F.when(F.col("Country").isNull(), "M")
	 .when(F.col("Country") == "", "M")
	 .otherwise("OK"))
	print(df.groupBy("qa_Country").count().distinct().orderBy("qa_Country", ascending=False).show()) 		

 
if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Online Retail]"))

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          .schema(schema_online_retail)
		          .load("/home/spark/capgemini-aceleracao-pyspark/data/online-retail/online-retail.csv"))

	qa_InvoiceNo(df)
	qa_StockCode(df)
	qa_Description(df)
	qa_Quantity(df)
	qa_InvoiceDate(df)
	qa_UnitPrice(df)
	qa_CustomerID(df)
	qa_Country(df)			  

	
	

	
