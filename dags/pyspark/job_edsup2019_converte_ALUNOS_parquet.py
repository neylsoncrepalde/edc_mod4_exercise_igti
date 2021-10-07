from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# set conf
conf = (
SparkConf()
    .set("spark.hadoop.fs.s3a.fast.upload", True)
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .set('spark.hadoop.fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.EnvironmentVariableCredentialsProvider')
    .set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.3')
)

# apply config
sc = SparkContext(conf=conf).getOrCreate()


if __name__ == "__main__":

    spark = SparkSession.builder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("Leitura dos dados....")
    df = (
        spark
        .read
        .format("csv")
        .options(header=True, inferSchema=True, delimiter="|", encoding="latin1")
        .load("s3a://dl-landing-zone-539445819060/edsup2019/aluno/")
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
            .when(col("TP_COR_RACA") == 5, "Indigena")
            .when(col("TP_COR_RACA") == 9, None)
            .when(col("TP_COR_RACA").isNull(), None)
        )
    )


    print("Escrita dos dados...")
    (
        df
        .write
        .parquet("s3a://dl-processing-zone-539445819060/edsup2019/aluno/", mode="overwrite")
    )