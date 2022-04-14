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
	(df.groupBy("qa_InvoiceNo").count().distinct().orderBy("qa_InvoiceNo", ascending=False).show())

def qa_StockCode(df):
	df = df.withColumn("qa_StockCode", 
	F.when(~F.col("StockCode").rlike("([0-9a-zA-Z]{5})"), "F")
	 .otherwise("OK"))
	(df.groupBy("qa_StockCode").count().distinct().orderBy("qa_StockCode", ascending=False).show())

def qa_Description(df):
	df = df.withColumn("qa_Description", 
	F.when(F.col("Description").isNull(), "M")
	 .when(F.col("Description") == "", "M")
	 .otherwise("OK"))
	(df.groupBy("qa_Description").count().distinct().orderBy("qa_Description", ascending=False).show())

def qa_Quantity(df):
	df = df.withColumn("qa_Quantity", 
	F.when(~F.col("Quantity").rlike("\d"), "N")
	 .otherwise("OK"))
	(df.groupBy("qa_Quantity").count().distinct().orderBy("qa_Quantity", ascending=False).show())		

def qa_CustomerID(df):
	df = df.withColumn("qa_CustomerID", 
	F.when(~F.col("CustomerID").rlike("([0-9a-zA-Z]{5})"), "F").otherwise("OK"))
	(df.groupBy("qa_CustomerID").count().distinct().orderBy("qa_CustomerID", ascending=False).show())

def qa_Country(df):
	df = df.withColumn("qa_Country", 
	F.when(F.col("Country").isNull(), "M")
	 .when(F.col("Country") == "", "M")
	 .otherwise("OK"))
	(df.groupBy("qa_Country").count().distinct().orderBy("qa_Country", ascending=False).show())

def pergunta_1(df):
	df = df.withColumn('UnitPrice', F.regexp_replace(F.col('UnitPrice'), ',', '.').cast('float'))
	(df.where(F.col('StockCode').rlike('gift_0001'))
			.agg(F.round(F.sum(F.col('UnitPrice') * F.col('Quantity')), 2).alias('total_gift_cards')).show())	

def pergunta_2(df):
	df = (df.withColumn('UnitPrice', F.regexp_replace(F.col('UnitPrice'), ',', '.').cast('float'))
			.withColumn("InvoiceDate", F.to_timestamp(F.col("InvoiceDate"), "d/M/yyyy H:m")))
	(df.where(F.col('StockCode').rlike('gift_0001').alias('Gift_Cards'))	
			.groupBy(F.month("InvoiceDate").alias('mes'))
			.agg(F.round(F.sum('UnitPrice'), 2).alias('total_gift_Cards_month'))
			.orderBy('mes').show())

def pergunta_3(df):

	df = (df.withColumn('UnitPrice', F.regexp_replace(F.col('UnitPrice'), ',', '.').cast('float'))
			.withColumn("UnitPrice", F.when(F.col("InvoiceNo").startswith('C'), 0).otherwise(F.col("UnitPrice")))
			)
	(df.where(F.col('StockCode')== 'S')
			.agg(F.round(F.sum(F.col('UnitPrice')), 2).alias('total_samples')).show())	

def pergunta_4(df):
	df = (df.withColumn("Quantity", F.when(F.col("Quantity").isNull() | (F.col("Quantity") < 0), 0)
			.otherwise(F.col("Quantity")))
			)
	(df.where(~F.col('StockCode').rlike('C'))
			.groupBy(F.col('Description'))
			.agg(F.sum('Quantity').alias('Quantity'))
			.orderBy(F.col('Quantity').desc())
			.limit(1)
			.show())
			
def pergunta_5(df):
	df = (df.withColumn("Quantity", F.when(F.col("Quantity").isNull() | (F.col("Quantity") < 0), 0)
			.otherwise(F.col("Quantity")))
			.withColumn("InvoiceDate", F.to_timestamp(F.col("InvoiceDate"), "d/M/yyyy H:m"))
			)	
	(df.where(~F.col('StockCode').rlike('C'))
			.groupBy('Description', F.month('InvoiceDate').alias('month'))
			.agg(F.sum('Quantity').alias('Quantity'))
			.orderBy(F.col('Quantity').desc()).dropDuplicates(['month'])
			.show())

def pergunta_6(df):
	df = (df.withColumn("Quantity", F.when(F.col("Quantity").isNull() | (F.col("Quantity") < 0), 0)
			.otherwise(F.col("Quantity")))
			.withColumn("InvoiceDate", F.to_timestamp(F.col("InvoiceDate"), "d/M/yyyy H:m"))
			.withColumn('UnitPrice', F.regexp_replace(F.col('UnitPrice'), ',', '.').cast('float'))
			.withColumn('UnitPrice', F.when(F.col('UnitPrice').isNull() | (F.col('UnitPrice') < 0), 0)
			.otherwise(F.col('UnitPrice')))
			)
	(df.where(~F.col('StockCode').rlike('C'))
			.groupBy(F.hour('InvoiceDate').alias('hour_sales'))
			.agg(F.round(F.sum(F.col('UnitPrice') * F.col('Quantity')), 2).alias('valor'))
			.orderBy(F.col('valor').desc())
			.limit(1)
			.show())

def pergunta_7(df):
	df = (df.withColumn("Quantity", F.when(F.col("Quantity").isNull() | (F.col("Quantity") < 0), 0)
			.otherwise(F.col("Quantity")))
			.withColumn("InvoiceDate", F.to_timestamp(F.col("InvoiceDate"), "d/M/yyyy H:m"))
			.withColumn('UnitPrice', F.regexp_replace(F.col('UnitPrice'), ',', '.').cast('float'))
			.withColumn('UnitPrice', F.when(F.col('UnitPrice').isNull() | (F.col('UnitPrice') < 0), 0)
			.otherwise(F.col('UnitPrice')))
			)
	(df.where(~F.col('StockCode').rlike('C'))
			.groupBy(F.month('InvoiceDate').alias('month_sales'))
			.agg(F.round(F.sum(F.col('UnitPrice') * F.col('Quantity')), 2).alias('valor'))
			.orderBy(F.col('valor').desc())
			.limit(1)
			.show())

def pergunta_8(df):
	df = (df.withColumn("Quantity", F.when(F.col("Quantity").isNull() | (F.col("Quantity") < 0), 0)
			.otherwise(F.col("Quantity")))
			.withColumn("InvoiceDate", F.to_timestamp(F.col("InvoiceDate"), "d/M/yyyy H:m"))
			.withColumn('UnitPrice', F.regexp_replace(F.col('UnitPrice'), ',', '.').cast('float'))
			.withColumn('UnitPrice', F.when(F.col('UnitPrice').isNull() | (F.col('UnitPrice') < 0), 0)
			.otherwise(F.col('UnitPrice')))
			)
	(df.where(~F.col('StockCode').rlike('C'))
			.groupBy('Description', F.year('InvoiceDate').alias('year'), F.month('InvoiceDate').alias('month'))
			.agg(F.round(F.sum(F.col('UnitPrice') * F.col('Quantity')), 2).alias('valor'))
			.orderBy(F.col('valor').desc()).dropDuplicates(['month'])
			.show())

def pergunta_9(df):
	df = (df.withColumn("Quantity", F.when(F.col("Quantity").isNull() | (F.col("Quantity") < 0), 0)
			.otherwise(F.col("Quantity")))
			.withColumn('UnitPrice', F.regexp_replace(F.col('UnitPrice'), ',', '.').cast('float'))
			.withColumn('UnitPrice', F.when(F.col('UnitPrice').isNull() | (F.col('UnitPrice') < 0), 0)
			.otherwise(F.col('UnitPrice')))
			)
	(df.where(~F.col('StockCode').rlike('C'))
			.groupBy(F.col('Country').alias('country_sales'))
			.agg(F.round(F.sum(F.col('UnitPrice') * F.col('Quantity')), 2).alias('valor'))
			.orderBy(F.col('valor').desc())
			.limit(1)
			.show())

def pergunta_10(df):
	df = (df.withColumn("Quantity", F.when(F.col("Quantity").isNull() | (F.col("Quantity") < 0), 0)
			.otherwise(F.col("Quantity")))
			.withColumn('UnitPrice', F.regexp_replace(F.col('UnitPrice'), ',', '.').cast('float'))
			.withColumn('UnitPrice', F.when(F.col('UnitPrice').isNull() | (F.col('UnitPrice') < 0), 0)
			.otherwise(F.col('UnitPrice')))
			)	
	(df.where(F.col('StockCode')=='M')
			.groupBy(F.col('Country').alias('manual_country_sales'))
			.agg(F.round(F.sum(F.col('UnitPrice') * F.col('Quantity')), 2).alias('valor'))
			.orderBy(F.col('valor').desc())
			.limit(1)
			.show())						

def pergunta_11(df):
	df = (df.withColumn("Quantity", F.when(F.col("Quantity").isNull() | (F.col("Quantity") < 0), 0)
			.otherwise(F.col("Quantity")))
			.withColumn('UnitPrice', F.regexp_replace(F.col('UnitPrice'), ',', '.').cast('float'))
			.withColumn('UnitPrice', F.when(F.col('UnitPrice').isNull() | (F.col('UnitPrice') < 0), 0)
			.otherwise(F.col('UnitPrice')))
			)
	(df.where(~F.col('StockCode').rlike('C'))
			.groupBy(F.col('InvoiceNo').alias('nf_sales'))
			.agg(F.round(F.sum(F.col('UnitPrice') * F.col('Quantity')), 2).alias('valor'))
			.orderBy(F.col('valor').desc())
			.limit(1)
			.show())

def pergunta_12(df):
	df = (df.withColumn("Quantity", F.when(F.col("Quantity").isNull() | (F.col("Quantity") < 0), 0)
			.otherwise(F.col("Quantity")))
			)
	(df.where(~F.col('StockCode').rlike('C'))
			.groupBy(F.col('InvoiceNo').alias('nf_itens'))
			.agg(F.sum(F.col('Quantity')).alias('total'))
			.orderBy(F.col('total').desc())
			.limit(1)
			.show())

def pergunta_13(df):
	(df.where(F.col("CustomerID").isNotNull())
			.groupBy(F.col('CustomerID').alias('customer'))
			.count()
			.orderBy(F.col('count').desc())
			.limit(1)
			.show())							
	
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
	qa_CustomerID(df)
	qa_Country(df)
	pergunta_1(df)
	pergunta_2(df)
	pergunta_3(df)
	pergunta_4(df)
	pergunta_5(df)
	pergunta_6(df)
	pergunta_7(df)
	pergunta_8(df)
	pergunta_9(df)
	pergunta_10(df)
	pergunta_11(df)
	pergunta_12(df)
	pergunta_13(df)