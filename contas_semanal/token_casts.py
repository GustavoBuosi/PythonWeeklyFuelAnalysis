from pyspark.sql.types import DateType, IntegerType
from pyspark.sql.functions import lit, col, regexp_replace, to_date
from pyspark.sql.types import DoubleType, DecimalType
from pyspark.sql import DataFrame


# Métodos utilizados para casts e parses simples, sem envolver cálculos.


def castFloats(df: DataFrame):
    return df.withColumn("PREÇO MÉDIO REVENDA",
                         regexp_replace("PREÇO MÉDIO REVENDA", ',', '.').cast(DecimalType(6, 3)))


def castDates(df: DataFrame):
    return df.withColumn("DATA INICIAL", to_date(df["DATA INICIAL"], "dd/MM/yyyy")) \
        .withColumn("DATA FINAL", to_date(df["DATA FINAL"], "dd/MM/yyyy"))



