from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

print("Leitura dos dados....")
df = (
    spark
    .read
    .format("csv")
    .options(header=True, inferSchema=True, delimiter="|", encoding="latin1")
    .load("s3://dl-landing-zone-539445819060/edsup2019/curso/")
)



print("Escrita dos dados...")
(
    df
    .write
    .parquet("s3://dl-processing-zone-539445819060/edsup2019/curso/", mode="overwrite")
)