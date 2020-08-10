from pyspark.sql.functions import *

def getWeightFactor(df: DataFrame):
    return df.withColumn("FATOR DE PESO", df["Peso da Semana"] / df["NUMERO DE DIAS NO MES CORRIGIDO"])


def aggregateValuesPerStateAndZone(df: DataFrame):
    return df.groupBy("ESTADO").agg(sum("PREÇO PONDERADO"))

def aggregateValuesPerCity(df: DataFrame):
    # return df.select("MÊS - CIDADE - PRODUTO", "PREÇO PONDERADO") \
    return df.select("ESTADO","REGIÃO","MUNICÍPIO","MÊS","PRODUTO","NÚMERO DE POSTOS PESQUISADOS", "PREÇO PONDERADO","PREÇO MÍNIMO REVENDA") \
        .groupBy("MUNICÍPIO","MÊS","PRODUTO","ESTADO","REGIÃO").agg(sum("PREÇO PONDERADO").alias("PREÇO PONDERADO"),
                                                                    sum("NÚMERO DE POSTOS PESQUISADOS"))

    # .groupBy("MÊS - CIDADE - PRODUTO").agg(sum("PREÇO PONDERADO"))


def getWeightedAveragePrice(df: DataFrame):
    return df.withColumn("PREÇO PONDERADO", df["FATOR DE PESO"] * df["PREÇO MÉDIO REVENDA"])


def concatenate(df: DataFrame):
    return df.withColumn("MÊS - CIDADE - PRODUTO", concat(df["MUNICÍPIO"],
                                                          lit(" "),df["MÊS"].cast(StringType()),
                                                          lit(" "),df["PRODUTO"]))

def correctNumberOfDays(df: DataFrame, date_min, date_max):
    return df.withColumn("NUMERO DE DIAS NO MES CORRIGIDO",
                         when(last_day(to_date(lit(date_min),"dd/MM/yyyy")) == df["MÊS"],datediff(df["MÊS"],to_date(lit(date_min),"dd/MM/yyyy")) + 1.00)
                         .otherwise(when(last_day(to_date(lit(date_max),"dd/MM/yyyy")) == df["MÊS"],dayofmonth(lit('2019-08-03')))
                                    .otherwise(df["NUMERO DE DIAS NO MES"]))
                                    )