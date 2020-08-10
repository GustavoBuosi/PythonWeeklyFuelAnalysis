from pyspark.sql import DataFrame
from pyspark.sql.functions import *

def getWeightedAveragePricePerRegion(df: DataFrame):
    return df.withColumn("PREÇO PONDERADO POR MÊS", df["PREÇO PONDERADO"] * df["NÚMERO DE POSTOS PESQUISADOS"] /
                         df["NÚMERO DE POSTOS PESQUISADOS TOTAL POR REGIÃO"]).withColumn(
        "PREÇO MÁXIMO PONDERADO POR MÊS", df["PREÇO PONDERADO MÁXIMO"] * df["NÚMERO DE POSTOS PESQUISADOS"] /
                         df["NÚMERO DE POSTOS PESQUISADOS TOTAL POR REGIÃO"]).withColumn(
        "PREÇO MÍNIMO PONDERADO POR MÊS", df["PREÇO PONDERADO MÍNIMO"] * df["NÚMERO DE POSTOS PESQUISADOS"] /
                         df["NÚMERO DE POSTOS PESQUISADOS TOTAL POR REGIÃO"])

def aggregateNumeroPostosRegiao(df: DataFrame):
    return df.groupBy("REGIÃO", "MÊS", "PRODUTO").agg(sum("NÚMERO DE POSTOS PESQUISADOS") \
                                                                .alias("NÚMERO DE POSTOS PESQUISADOS TOTAL POR REGIÃO")) 


def getTotalNumberOfPostsPolledPerRegion(dfA: DataFrame, dfPesosPorRegiao: DataFrame):
    return dfA.join(dfPesosPorRegiao, (dfA["REGIÃO"] == dfPesosPorRegiao["REGIÃO"]) &
                    (dfA["MÊS"] == dfPesosPorRegiao["MÊS"]) & (dfA["PRODUTO"] == dfPesosPorRegiao["PRODUTO"])
                    ).select(dfA["MÊS"],dfA["PRODUTO"],dfA["REGIÃO"],
                             dfA["PREÇO PONDERADO"],dfA["NÚMERO DE POSTOS PESQUISADOS"],
                             dfPesosPorRegiao["NÚMERO DE POSTOS PESQUISADOS TOTAL POR REGIÃO"],
                             dfA["PREÇO PONDERADO MÁXIMO"],dfA["PREÇO PONDERADO MÍNIMO"])

def getMonthlyPricePerRegion(df: DataFrame):
    return df.groupBy("REGIÃO", "PRODUTO", "MÊS").agg(
        sum("PREÇO PONDERADO POR MÊS").alias("PREÇO PONDERADO POR MÊS"),
        sum("PREÇO MÁXIMO PONDERADO POR MÊS").alias("PREÇO MÁXIMO PONDERADO"),
        sum("PREÇO MÍNIMO PONDERADO POR MÊS").alias("PREÇO MÍNIMO PONDERADO")
    )

