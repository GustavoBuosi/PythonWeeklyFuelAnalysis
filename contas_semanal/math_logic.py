from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, DoubleType
from pyspark.sql.functions import *
from rows.row_rules import *

# Função criada para adicionar uma coluna com a última data de um mês
# e um número inteiro com o número de dias do mesmo.
def getLastDateFromMonth(df: DataFrame):
    dfLastDate = df.withColumn("DATA FINAL DO MÊS PRIMEIRO DIA",
                               last_day(to_date(col("DATA INICIAL"), "dd/MM/yyyy"))) \
        .withColumn("DATA FINAL DO MÊS ÚLTIMO DIA",
                    last_day(to_date(col("DATA FINAL"), "dd/MM/yyyy")))
    dfNumberOfDays = dfLastDate.withColumn("NÚMERO DE DIAS NO MÊS PRIMEIRO DIA",
                                           dfLastDate["DATA FINAL DO MÊS PRIMEIRO DIA"].cast(StringType())) \
        .withColumn("NÚMERO DE DIAS NO MÊS ÚLTIMO DIA",
                    dfLastDate["DATA FINAL DO MÊS ÚLTIMO DIA"].cast(StringType()))
    return dfNumberOfDays.withColumn("NÚMERO DE DIAS NO MÊS PRIMEIRO DIA",
                                     substring(dfNumberOfDays["NÚMERO DE DIAS NO MÊS PRIMEIRO DIA"], 9, 10).cast(
                                         DoubleType())) \
        .withColumn("NÚMERO DE DIAS NO MÊS ÚLTIMO DIA",
                    substring(dfNumberOfDays["NÚMERO DE DIAS NO MÊS ÚLTIMO DIA"], 9, 10).cast(DoubleType()))


def calculateWeightInAMonth(df: DataFrame):
    return df.withColumn("Peso da Semana 1", when(df["MÊSDIAINICIAL"] == df ["MÊSDIAFINAL"], lit(7.00)).otherwise(
        datediff(df["DATA FINAL DO MÊS PRIMEIRO DIA"], df["DATA INICIAL"]) + 1.00)) \
        .withColumn("Peso da Semana 2", when(df["MÊSDIAINICIAL"] != df["MÊSDIAFINAL"], dayofmonth("DATA FINAL")).otherwise(lit(0.00)))


def getMonth(df: DataFrame):
    return df.withColumn("MÊSDIAINICIAL", month(df["DATA INICIAL"])) \
        .withColumn("MÊSDIAFINAL", month(df["DATA FINAL"]))

def breakLines(df: DataFrame):
    return df.rdd.flatMap(lambda row: [rowsForFirstMonth(row),rowsForSecondMonth(row)] \
        if (row["MÊSDIAINICIAL"] != row["MÊSDIAFINAL"]) else [rowsForFirstMonth(row)]).toDF(newRowNames())



