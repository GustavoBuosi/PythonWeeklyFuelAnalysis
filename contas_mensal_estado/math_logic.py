from pyspark.sql import DataFrame

def aggregateNumeroPostos(df: DataFrame):
    return df.groupBy("MUNICÍPIO","MÊS","PRODUTO","ESTADO","REGIÃO").agg(sum("NÚMERO DE POSTOS PESQUISADOS"))