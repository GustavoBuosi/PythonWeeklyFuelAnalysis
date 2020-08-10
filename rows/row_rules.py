from pyspark.sql import Row

def rowsForFirstMonth(row: Row):
    return Row(row["REGIÃO"],
        row["ESTADO"],
        row["MUNICÍPIO"],
        row["PRODUTO"],
        row["NÚMERO DE POSTOS PESQUISADOS"],
        row["PREÇO MÉDIO REVENDA"],
        row["DESVIO PADRÃO REVENDA"],
        row["PREÇO MÍNIMO REVENDA"],
        row["PREÇO MÁXIMO REVENDA"],
        row["MARGEM MÉDIA REVENDA"],
        row["Peso da Semana 1"],
        row["DATA FINAL DO MÊS PRIMEIRO DIA"],
        row["NÚMERO DE DIAS NO MÊS PRIMEIRO DIA"]
        )

def rowsForSecondMonth(row: Row):
    return Row(row["REGIÃO"],
        row["ESTADO"],
        row["MUNICÍPIO"],
        row["PRODUTO"],
        row["NÚMERO DE POSTOS PESQUISADOS"],
        row["PREÇO MÉDIO REVENDA"],
        row["DESVIO PADRÃO REVENDA"],
        row["PREÇO MÍNIMO REVENDA"],
        row["PREÇO MÁXIMO REVENDA"],
        row["MARGEM MÉDIA REVENDA"],
        row["Peso da Semana 2"],
        row["DATA FINAL DO MÊS ÚLTIMO DIA"],
        row["NÚMERO DE DIAS NO MÊS ÚLTIMO DIA"]
        )

def newRowNames():
    return ["REGIÃO",
        "ESTADO",
        "MUNICÍPIO",
        "PRODUTO",
        "NÚMERO DE POSTOS PESQUISADOS",
        "PREÇO MÉDIO REVENDA",
        "DESVIO PADRÃO REVENDA",
        "PREÇO MÍNIMO REVENDA",
        "PREÇO MÁXIMO REVENDA",
        "MARGEM MÉDIA REVENDA",
        "Peso da Semana",
        "MÊS",
        "NUMERO DE DIAS NO MES"]