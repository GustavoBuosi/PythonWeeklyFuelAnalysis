from pyspark.sql import SparkSession,Row
from pyspark.sql.functions import col
from rows.row_rules import *
from contas_mensal_cidade.math_logic import *
from contas_semanal.token_casts import *
from contas_semanal.math_logic import *
import os

if __name__ == "__main__":

    # Data mínima:
    date_min = '30/12/2018'
    # Data máxima:
    date_max = '3/8/2019'
    os.chdir("/home/gustavo/PycharmProjects/PySparkWeeklyFuelAnalysis")
    spark = SparkSession.builder.appName("FuelAnalysis").master("local[*]").getOrCreate()
    df = spark.read.format("csv") \
        .option("header","true") \
        .load("SEMANAL_MUNICIPIOS-2019.csv")

    # Modificando delimitadores:
    df = castFloats(df)
    df = castDates(df)
    # Capturando a última data de um mês para os dois meses correspondentes. Aqui é criada a coluna "DATA FINAL DO MÊS":
    df = getLastDateFromMonth(df)
    # df = getTotalDatesFromMonth(df)
    # # Capturando os meses correspondentes dos campos de data:
    df = getMonth(df)
    df = calculateWeightInAMonth(df)
    df = breakLines(df)
    df = correctNumberOfDays(df, date_min, date_max)
    df = getWeightFactor(df)
    df = getWeightedAveragePrice(df)
    df = concatenate(df)
    # Resultado para a questão (a):
    dfA = aggregateValuesPerCity(df)
    # Resultado para questão (b), aproveitando o dfA processado:
    # dfB = aggregateValuesPerStateAndZone(dfA)

    # df.printSchema()
    # df.show()
    dfA\
        .coalesce(1)\
        .write\
        .option("header","true")\
        .option("sep",",")\
        .mode("overwrite")\
        .csv("file:///home/gustavo/PycharmProjects/PySparkWeeklyFuelAnalysis/results.csv")