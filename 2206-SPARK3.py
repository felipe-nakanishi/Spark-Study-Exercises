# Databricks notebook source
from pyspark.sql.functions import *
df = spark.read.csv("/FileStore/tables/netflix_titles.csv", header = 'true', inferSchema = 'true')

# COMMAND ----------

#Desafio 1
#Os valores da coluna Cast é representada por uma String contendo todo o elenco do show, porém queremos que cada pessoa desse elenco seja representada por coluna


df2 = df.select(df.show_id, split(df.cast, ',').alias('array'))
display(df2.select(df2.show_id, posexplode(df2.array)).groupby('show_id').pivot('pos').agg(first('col')))


# COMMAND ----------

#Desafio 2
#Gostaríamos de saber todos os atores/atrizes que participaram de algum show com o diretor “Adam Shankman”.
df3 = df.select('director', split('cast', ',').alias('castarray'))
display(df3.select('director', explode('castarray')).filter('director == "Adam Shankman"'))

# COMMAND ----------

#Desafio 3
#Criar uma UDF que conte o número de vogais no nome dos diretores. Obs: Tratar Valores nulos.
def conta_vogais(nome):
  contagem = 0
  if nome is not None:
    for i in nome:
      if i.upper() in ['A','E','I','O','U']:
        contagem+=1
  else:
    contagem = 0

  return contagem

contavogais = udf(lambda x:conta_vogais(x))

display(df.select('director', contavogais('director').alias('countVowels')))

# COMMAND ----------


