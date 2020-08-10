from pyspark.sql import DataFrame
from pyspark.sql.functions import *

def aggregateNumeroPostosEstado(df: DataFrame):
    return df.groupBy("ESTADO","REGIÃO","MÊS","PRODUTO").agg(sum("NÚMERO DE POSTOS PESQUISADOS") \
                                                                         .alias("NÚMERO DE POSTOS PESQUISADOS TOTAL POR ESTADO"))

def getTotalNumberOfPostsPolledPerState(dfA: DataFrame, dfPesosPorEstado: DataFrame):
    return dfA.join(dfPesosPorEstado, (dfA["ESTADO"] == dfPesosPorEstado["ESTADO"]) &
                    (dfA["MÊS"] == dfPesosPorEstado["MÊS"]) & (dfA["PRODUTO"] == dfPesosPorEstado["PRODUTO"])
                    ).select(dfA["ESTADO"],dfA["MÊS"],dfA["PRODUTO"],dfA["REGIÃO"],
                             dfA["PREÇO PONDERADO"],dfA["NÚMERO DE POSTOS PESQUISADOS"],
                             dfPesosPorEstado["NÚMERO DE POSTOS PESQUISADOS TOTAL POR ESTADO"],
                             dfA["PREÇO PONDERADO MÁXIMO"],dfA["PREÇO PONDERADO MÍNIMO"])

def getWeightedAveragePricePerState(df: DataFrame):
    return df.withColumn("PREÇO PONDERADO POR MÊS", df["PREÇO PONDERADO"] * df["NÚMERO DE POSTOS PESQUISADOS"] /
                         df["NÚMERO DE POSTOS PESQUISADOS TOTAL POR ESTADO"]).withColumn(
        "PREÇO MÁXIMO PONDERADO POR MÊS", df["PREÇO PONDERADO MÁXIMO"] * df["NÚMERO DE POSTOS PESQUISADOS"] /
                         df["NÚMERO DE POSTOS PESQUISADOS TOTAL POR ESTADO"]).withColumn(
        "PREÇO MÍNIMO PONDERADO POR MÊS", df["PREÇO PONDERADO MÍNIMO"] * df["NÚMERO DE POSTOS PESQUISADOS"] /
                         df["NÚMERO DE POSTOS PESQUISADOS TOTAL POR ESTADO"])


def getMonthlyPricePerState(df: DataFrame):
    return df.groupBy("ESTADO","MÊS","PRODUTO","REGIÃO").agg(
        sum("PREÇO PONDERADO POR MÊS").alias("PREÇO PONDERADO POR MÊS"),
        sum("PREÇO MÁXIMO PONDERADO POR MÊS").alias("PREÇO MÁXIMO PONDERADO"),
        sum("PREÇO MÍNIMO PONDERADO POR MÊS").alias("PREÇO MÍNIMO PONDERADO")
    )

