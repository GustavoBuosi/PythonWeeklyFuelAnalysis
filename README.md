# Introdução

Este repositório visa realizar uma análise de dados de valores de combustíveis. <br/>

Com base em dados semanais obtidos em cada município, verifica-se o peso
que cada linha possui em um determinado mês com base na semana que ela representa. <br/>

Por exemplo:

- A primeira linha do CSV de origem possui dados referentes a 30/12/2018 a 05/01/2019.
Assim, na ponderação dos valores para o mês de dezembro, haverá um peso de 2/31 (6.4%)
para os valores contidos nesta linha, com um peso de 5/31 (16.1%) para o mês de janeiro, 
porém com um peso de 2/7 da semana;
- As linhas da próxima semana englobam de 06/01/2019 a 12/01/2019. Essas linhas, por
representarem apenas uma semana, possuem

### Tratamento de dados

Inicialmente, é feito um cast das varíáveis de preço para DoubleType (com o respectivo
tratamento dos delimitadores) e a obtenção dos últimos dias de cada mês para a ponderação. </br>

```python
    # Modificando delimitadores:
    df = castFloats(df)
    df = castDates(df)
    # Capturando a última data de um mês para os dois meses correspondentes. Aqui é criada a coluna "DATA FINAL DO MÊS":
    df = getLastDateFromMonth(df)
    # df = getTotalDatesFromMonth(df)
    # # Capturando os meses correspondentes dos campos de data:
    df = getMonth(df)
```

## Quebrando linhas e corrigindo o número de dias no primeiro e último mês

Com esses valores, faz-se uma função para quebrar uma linha, caso ela incorpore datas de dois meses,
ou mantê-la intacta, caso esteja contida em um mês. Para as linhas que forem quebradas,
são considerados os pesos respectivos corretos para o mês que representam:

```python
# df = breakLines(df)
def breakLines(df: DataFrame):
    return df.rdd.flatMap(lambda row: [rowsForFirstMonth(row),rowsForSecondMonth(row)] \
        if (row["MÊSDIAINICIAL"] != row["MÊSDIAFINAL"]) else [rowsForFirstMonth(row)]).toDF(newRowNames())
```

Como o primeiro e o último mês não possuem dados para todos os dias, realiza-se uma correção
do número de meses para eles.

```python
df = correctNumberOfDays(df, date_min, date_max)
```

## Agregação por mês

Em seguida, é feita a agregação, multiplicando pelos respectivos fatores corrigidos:

```python
    df = getWeightFactor(df)
    df = getWeightedAveragePrice(df)
    # Resultado para a questão (a):
    dfA = aggregateValuesPerCity(df)

    dfA\
        .coalesce(1)\
        .write\
        .option("header","true")\
        .option("sep",",")\
        .mode("overwrite")\
        .csv("file:///home/gustavo/PycharmProjects/PySparkWeeklyFuelAnalysis/resultsA.csv")
```

## Valores por estado (completo) e valores por região (a corrigir)

Para os valores mensais, não é possível desprezar o número de postos pesquisado por cidade. Com isso,
faz-se uma agregação para determinar o número total de postas pesquisados por estado:

```python
dfPesosPorEstado = aggregateNumeroPostosEstado(dfA)

dfBAux1 = getTotalNumberOfPostsPolledPerState(dfA,dfPesosPorEstado)
```

Para realizar um join das colunas com o DataFrame semanal, fazendo um novo fator de peso ponderado por Estado. 

```python
dfBAux1 = getWeightedAveragePricePerState(dfBAux1)

    dfB1 = getMonthlyPricePerState(dfBAux1)

    dfB1 \
        .coalesce(1) \
        .write \
        .option("header", "true") \
        .option("sep", ",") \
        .mode("overwrite") \
        .csv("file:///home/gustavo/PycharmProjects/PySparkWeeklyFuelAnalysis/resultsB1.csv")
```

## Pontos a melhorar no projeto:
- Considerar, para os valores semanais, o número de municípios entrevistados em cada cidade;
- Fazer a análise mensal por municípios.
