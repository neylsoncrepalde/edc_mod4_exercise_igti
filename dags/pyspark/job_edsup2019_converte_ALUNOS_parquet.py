from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder.getOrCreate()

print("Leitura dos dados....")
df = (
    spark
    .read
    .format("csv")
    .options(header=True, inferSchema=True, delimiter="|", encoding="latin1")
    .load("s3://dl-landing-zone-539445819060/edsup2019/aluno/")
)


print("Tratamento dos dados...")
df = (
    df
    .withColumn("TP_SEXO", col("TP_SEXO").cast("string"))
    .withColumn("TP_SEXO", 
        when(col("TP_SEXO") == '1', "F")
        .otherwise("M")
    )
)

df = (
    df
    .withColumn("TP_COR_RACA", col("TP_COR_RACA").cast("string"))
    .withColumn("TP_COR_RACA", 
        when(col("TP_COR_RACA") == 0, "ND")
        .when(col("TP_COR_RACA") == 1, "Branca")
        .when(col("TP_COR_RACA") == 2, "Preta")
        .when(col("TP_COR_RACA") == 3, "Parda")
        .when(col("TP_COR_RACA") == 4, "Amarela")
        .when(col("TP_COR_RACA") == 5, "Indígena")
        .when(col("TP_COR_RACA") == 9, None)
        .when(col("TP_COR_RACA").isNull(), None)
    )
)


print("Escrita dos dados...")
(
    df
    .write
    .parquet("s3://dl-processing-zone-539445819060/edsup2019/aluno/", mode="overwrite")
)