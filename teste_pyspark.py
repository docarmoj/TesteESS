# Importando pacotes necessários
from pyspark.sql import SparkSession 
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F
from pyspark.sql.window import Window as W

# Gerando um array de dados a serem consumidos
data = [("125","São Paulo","SP","2021-01-08"),
("1258","São Paulo","SP","2021-01-13"),
("654","São Paulo","SP","2020-01-12"),
("600","Campinas","SP","2021-02-18"),
("588","Campinas","SP","2021-01-15"),
("325","Garanhuns","PE","2020-12-12"),
("624","Blumenau","SC","2020-12-12"),
("700","Porto de Galinhas","PE","2021-03-18"),
("989","Fortaleza","CE","2021-02-17"),
("1054","Fortaleza","CE","2021-02-01"),
("1250","Pomerode","SC","2021-01-25"),
("259","Pomerode","SC","2021-01-22"),
("785","Salvador","BA","2021-02-13")  ]

# Definição da estrutura e do tipo de dado
schema = StructType([ \
    StructField("Transacao",StringType(),True), \
    StructField("Municipio",StringType(),True), \
    StructField("Estado",StringType(),True), \
    StructField("Data_Atualizacao",StringType(), True)
    ])
 
# Ponto de entrada para gerar uma sessão do spark
spark = SparkSession.builder.appName("PySpark Test").getOrCreate()

# ==> Lendo de um arquivo CSV(local) sem a necessidade de gerar um schema
# ==> df = spark.read.csv(r'./resources/data.csv',header=True)

# Criando um DataFrame passando os valores do array e o schema definido
df = spark.createDataFrame(data=data,schema=schema)


# Criando um novo DataFrame que irá incluir uma nova coluna, 
# convertendo campo String em date 
# e excluindo a coluna de data(origem em string)
df2 = df.withColumn('Atualizacao', F.to_date(F.unix_timestamp(F.col('Data_Atualizacao'), 'yyyy-MM-dd').cast("timestamp"))).drop('Data_Atualizacao')

# Criando o Dataframe final adcionando uma nova coluna
#  que fará o rank ordenando pela data de atualização,
#  agrupado por municipio e ordenado
df_final = df2.withColumn("Ordem Transacao",F.dense_rank().over(W.partitionBy("Municipio").orderBy("Atualizacao"))).orderBy("Municipio", "Ordem Transacao") 

# Exibindo resultado
df_final.show(truncate=False)