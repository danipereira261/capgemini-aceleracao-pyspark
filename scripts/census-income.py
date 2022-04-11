from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

schema_census_income = StructType()([
	StructField("age", IntegerType(), True),
	StructField("workclass", StringType(), True),
	StructField("fnlwgt", FloatType(), True),
	StructField("education", StringType(), True),
	StructField("education-num", IntegerType(), True),
	StructField("marital-status", StringType(), True),
	StructField("occupation", StringType(), True),
	StructField("relashionship", StringType(), True),
	StructField("race", StringType(), True),
	StructField("sex", StringType(), True),
	StructField("capital-gain", FloatType(), True),
	StructField("capital-loss", FloatType(), True),
	StructField("hours-per-week", IntegerType(), True),
	StructField("native-country", StringType(), True),
	StructField("income", StringType(), True),

])

if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Census Income]"))		

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          .schema(schema_census_income)
		          .load("/home/spark/capgemini-aceleracao-pyspark/data/census-income/census-income.csv"))
	print(df.show())
