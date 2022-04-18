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

# Função de transformação


def online_retail_tr(df):

    # Tratamento Quantity
    df = (df.withColumn('Quantity', F.when(F.col('Quantity').isNull(), 0)
                        .when(F.col('Quantity') < 0, 0)
                        .otherwise(F.col('Quantity')))
          )

    # Tratamento InvoiceDate
    df = (df.withColumn('InvoiceDate', F.to_timestamp(F.col('InvoiceDate'), 'd/M/yyyy H:m'))
          )

    # Transformação UnitPrice
    df = (df.withColumn('UnitPrice', F.regexp_replace(F.col('UnitPrice'), ',', '.').cast('float'))
          .withColumn("UnitPrice", F.when(F.col("InvoiceNo").startswith('C'), 0).otherwise(F.col("UnitPrice")))
          .withColumn('UnitPrice',
                      F.when((F.col('UnitPrice').isNull())
                             | (F.col('UnitPrice') < 0), 0)
                      .otherwise(F.col('UnitPrice')))
          )

    return df


def pergunta_1(df):
    (df.where(F.col('StockCode').rlike('gift_0001'))
     .agg(F.round(F.sum(F.col('UnitPrice') * F.col('Quantity')), 2).alias('valor')).show())


def pergunta_2(df):
    (df.where(F.col('StockCode').rlike('gift_0001').alias('Gift_Cards'))
     .groupBy(F.month("InvoiceDate").alias('mes'))
     .agg(F.round(F.sum('UnitPrice'), 2).alias('total_gift_Cards_month'))
     .orderBy('mes').show())


def pergunta_3(df):
    df = (df.withColumn('UnitPrice', F.regexp_replace(F.col('UnitPrice'), ',', '.').cast('float'))
          .withColumn("UnitPrice", F.when(F.col("InvoiceNo").startswith('C'), 0).otherwise(F.col("UnitPrice")))
          )
    (df.where(F.col('StockCode') == 'S')
     .agg(F.round(F.sum(F.col('UnitPrice')), 2).alias('total_samples')).show())


def pergunta_4(df):
    (df.where(~F.col('StockCode').rlike('C'))
     .groupBy(F.col('Description'))
     .agg(F.sum('Quantity').alias('Quantity'))
     .orderBy(F.col('Quantity').desc())
     .limit(1)
     .show())


def pergunta_5(df):
    (df.where(~F.col('InvoiceNo').rlike('C'))
     .groupBy('Description', F.month('InvoiceDate').alias('month'))
     .agg(F.sum('Quantity').alias('Quantity'))
     .orderBy(F.col('Quantity').desc()).dropDuplicates(['month'])
     .show())


def pergunta_6(df):
    (df.where(~F.col('InvoiceNo').rlike('C'))
     .groupBy(F.hour('InvoiceDate').alias('hour_sales'))
     .agg(F.round(F.sum(F.col('UnitPrice') * F.col('Quantity')), 2).alias('valor'))
     .orderBy(F.col('valor').desc())
     .limit(1)
     .show())


def pergunta_7(df):
    (df.where(~F.col('InvoiceNo').rlike('C'))
     .groupBy(F.month('InvoiceDate').alias('month_sales'))
     .agg(F.round(F.sum(F.col('UnitPrice') * F.col('Quantity')), 2).alias('valor'))
     .orderBy(F.col('valor').desc())
     .limit(1)
     .show())


def pergunta_8(df):
    (df.where(~F.col('InvoiceNo').rlike('C'))
     .groupBy('Description', F.year('InvoiceDate').alias('year'), F.month('InvoiceDate').alias('month'))
     .agg(F.round(F.sum(F.col('UnitPrice') * F.col('Quantity')), 2).alias('valor'))
     .orderBy(F.col('valor').desc()).dropDuplicates(['month'])
     .show())


def pergunta_9(df):
    (df.where(~F.col('InvoiceNo').rlike('C'))
     .groupBy(F.col('Country').alias('country_sales'))
     .agg(F.round(F.sum(F.col('UnitPrice') * F.col('Quantity')), 2).alias('valor'))
     .orderBy(F.col('valor').desc())
     .limit(1)
     .show())


def pergunta_10(df):
    (df.where(F.col('StockCode') == 'M')
     .groupBy(F.col('Country').alias('manual_country_sales'))
     .agg(F.round(F.sum(F.col('UnitPrice') * F.col('Quantity')), 2).alias('valor'))
     .orderBy(F.col('valor').desc())
     .limit(1)
     .show())


def pergunta_11(df):
    (df.where(~F.col('StockCode').rlike('C'))
     .groupBy(F.col('InvoiceNo').alias('nf_sales'))
     .agg(F.round(F.sum(F.col('UnitPrice') * F.col('Quantity')), 2).alias('valor'))
     .orderBy(F.col('valor').desc())
     .limit(1)
     .show())


def pergunta_12(df):
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
    spark = (SparkSession.builder.appName(
        "Aceleração PySpark - Capgemini [Online Retail]"))

    df = (spark.getOrCreate().read
          .format("csv")
          .option("header", "true")
          .schema(schema_online_retail)
          .load("/home/spark/capgemini-aceleracao-pyspark/data/online-retail/online-retail.csv"))

    df_tr = online_retail_tr(df)

    pergunta_1(df_tr)
    pergunta_2(df_tr)
    pergunta_3(df_tr)
    pergunta_4(df_tr)
    pergunta_5(df_tr)
    pergunta_6(df_tr)
    pergunta_7(df_tr)
    pergunta_8(df_tr)
    pergunta_9(df_tr)
    pergunta_10(df_tr)
    pergunta_11(df_tr)
    pergunta_12(df_tr)
    pergunta_13(df_tr)
