from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

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
        .load("s3a://dl-landing-zone-539445819060/edsup2019/curso/")
    )



    print("Escrita dos dados...")
    (
        df
        .write
        .parquet("s3a://dl-processing-zone-539445819060/edsup2019/curso/", mode="overwrite")
    )