from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

schema_census_income = StructType([
    StructField("age", IntegerType(), True),
    StructField("workclass", StringType(), True),
    StructField("fnlwgt", IntegerType(), True),
    StructField("education", StringType(), True),
    StructField("education-num", IntegerType(), True),
    StructField("marital-status", StringType(), True),
    StructField("occupation", StringType(), True),
    StructField("relashionship", StringType(), True),
    StructField("race", StringType(), True),
    StructField("sex", StringType(), True),
    StructField("capital-gain", IntegerType(), True),
    StructField("capital-loss", IntegerType(), True),
    StructField("hours-per-week", IntegerType(), True),
    StructField("native-country", StringType(), True),
    StructField("income", StringType(), True)

])

# Função de transformação


def census_income_tr(df):

    # Tratamento Workclass
    df = (df.withColumn('workclass', F.when(F.col('workclass').rlike('\?'), None)
                        .otherwise(F.col('workclass')))
          )
    # Tratamento Occupation
    df = (df.withColumn('occupation', F.when(F.col('occupation').rlike('\?'), None)
                        .otherwise(F.col('occupation')))
          )
    # Tratamento Native-Country
    df = (df.withColumn('native-country', F.when(F.col('native-country').rlike('\?'), None)
                        .otherwise(F.col('native-country')))
          )

    # Coluna Auxiliar casado-status
    df = (df.withColumn('casado-status', F.when(F.col('marital-status').rlike('Married-civ-spouse'), 'casado')
                        .when(F.col('marital-status').rlike('Married-AF'), 'casado')
                        .when(F.col('marital-status').rlike('Married-spouse-absent'), 'casado')
                        .otherwise('nao-casado'))
          )

    return df


def pergunta_1(df):
    print('Pergunta - 1')

    (df.where(F.col('workclass').isNotNull() & F.col('income').rlike('\>50K'))
     .groupBy(F.col('workclass'), F.col('income'))
     .count()
     .orderBy(F.col('count').desc())
     .limit(1)
     .show()
     )


def pergunta_2(df):
    print('Pergunta - 2')

    (df.groupBy(F.col('race'))
     .agg(F.round(F.avg(F.col('hours-per-week'))).alias('media_hora_trabalho_semanal'))
     .orderBy(F.col('media_hora_trabalho_semanal').desc())
     .show()
     )


def pergunta_3(df):
    print('Pergunta - 3')

    (df.groupBy('sex')
     .agg(F.count(F.col('sex')).alias('total'))
     .withColumn('proporcao', F.round((F.col('total')/df.count()), 2))
     .show()
     )


def pergunta_5(df):
    print('Pergunta - 5')

    (df.where(F.col('occupation').isNotNull())
     .groupBy(F.col('occupation'))
     .agg(F.round(F.avg(F.col('hours-per-week'))).alias('media_hora_trabalho_semanal'))
     .orderBy(F.col('media_hora_trabalho_semanal').desc())
     .limit(1)
     .show()
     )


def pergunta_6(df):
    print('Pergunta - 6')

    (df.where(F.col('occupation').isNotNull())
     .groupBy(F.col('occupation'), F.col('education'))
     .agg(F.count(F.col('occupation')).alias('count'))
     .orderBy(F.col('count').desc())
     .limit(1)
     .show()
     )


def pergunta_7(df):
    print('Pergunta - 7')

    (df.where(F.col('occupation').isNotNull())
     .groupBy(F.col('occupation'), F.col('sex'))
     .agg(F.count(F.col('occupation')).alias('count'))
     .orderBy(F.col('count').desc())
     .limit(1)
     .show()
     )


def pergunta_8(df):
    print('Pergunta - 8')

    (df.where(F.col('education').rlike('Doctorate'))
     .groupBy(F.col('education'), F.col('race'))
     .agg(F.count(F.col('race')).alias('count'))
     .orderBy(F.col('count').desc())
     .show()
     )


def pergunta_9(df):
    print('Pergunta - 9')

    df = (df.withColumn('self-employed', F.when(F.col('workclass').rlike('Self-emp-not-inc'), 'self-employed')
                        .when(F.col('workclass').rlike(' Self-emp-inc'), 'self-employed'))
          )

    (df.where(F.col('self-employed').rlike('self-employed'))
     .groupBy(F.col('education'), F.col('sex'), F.col('race'))
     .agg(F.count(F.col('self-employed')).alias('count'))
     .orderBy(F.col('count').desc())
     .limit(1)
     .show()
     )


def pergunta_10(df):
    print('Pergunta - 10')

    (df.groupBy('casado-status')
     .agg(F.count(F.col('casado-status')).alias('total'))
     .withColumn('razao', F.round((F.col('total')/df.count()), 2))
     .show()
     )


def pergunta_11(df):
    print('Pergunta - 11')

    (df.where(F.col('casado-status').rlike('nao-casado'))
     .groupBy(F.col('race'), F.col('casado-status'))
     .agg(F.count(F.col('race')).alias('count'))
     .orderBy(F.col('count').desc())
     .limit(1)
     .show()
     )


def pergunta_12(df):
    print('Pergunta - 12')

    (df.groupBy(F.col('income'), F.col('casado-status'))
     .agg(F.count(F.col('casado-status')).alias('count'))
     .orderBy(F.col('count').desc())
     .limit(2)
     .show()
     )


def pergunta_13(df):
    print('Pergunta - 13')

    (df.groupBy(F.col('income'), F.col('sex'))
     .agg(F.count(F.col('sex')).alias('count'))
     .orderBy(F.col('count').desc())
     .limit(2)
     .show()
     )


def pergunta_14(df):
    print('Pergunta - 14')

    (df.where(F.col('native-country').isNotNull())
     .groupBy(F.col('native-country'), F.col('income'))
     .agg(F.count(F.col('native-country')).alias('count'))
     .orderBy(F.col('count').desc())
     .show()
     )


def pergunta_15(df):
    print('Pergunta - 15')

    # Coluna Auxiliar brancas-nao-brancas
    df = (df.withColumn('brancas-nao-brancas', F.when(F.col('race').rlike('White'), 'brancas')
                        .otherwise('nao-brancas'))
          )

    (df.groupBy('brancas-nao-brancas')
     .agg(F.count(F.col('brancas-nao-brancas')).alias('total'))
     .withColumn('razao', F.round((F.col('total')/df.count()), 2))
     .show()
     )


if __name__ == "__main__":
    sc = SparkContext()
    spark = (SparkSession.builder.appName(
        "Aceleração PySpark - Capgemini [Census Income]"))

    df = (spark.getOrCreate().read
          .format("csv")
          .option("header", "true")
          .schema(schema_census_income)
          .load("../capgemini-aceleracao-pyspark/data/census-income/census-income.csv"))

    df_tr = census_income_tr(df)

    pergunta_1(df_tr)
    pergunta_2(df_tr)
    pergunta_3(df_tr)
    pergunta_5(df_tr)
    pergunta_6(df_tr)
    pergunta_7(df_tr)
    pergunta_8(df_tr)
    pergunta_9(df_tr)
    pergunta_10(df_tr)
    pergunta_11(df_tr)
    pergunta_12(df_tr)
    pergunta_13(df_tr)
    pergunta_14(df_tr)
    pergunta_15(df_tr)
