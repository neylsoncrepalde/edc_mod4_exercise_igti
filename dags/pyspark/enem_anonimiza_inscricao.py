from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat, substring

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

    # init spark session
    spark = SparkSession\
            .builder\
            .appName("ENEM Job")\
            .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark
        .read
        .format("parquet")
        .load("s3a://dl-processing-zone-539445819060/enem/")
    )
    
    print("*************")
    print("* ANONIMIZA *")
    print("*************")

    inscricao_oculta = (
        df
        .withColumn("inscricao_string", df.NU_INSCRICAO.cast("string"))
        .withColumn("inscricao_menor", substring(col("inscricao_string"), 5, 4))
        .withColumn("inscricao_oculta", concat(lit("*****"), col("inscricao_menor"), lit("***")))
        .select("NU_INSCRICAO", "inscricao_oculta", "NU_NOTA_MT", "SG_UF_RESIDENCIA")
    )

    (
        inscricao_oculta
        .write
        .mode("overwrite")
        .format("parquet")
        .save("s3a://dl-consumer-zone-539445819060/enem_anon/")
    )

    print("*********************")
    print("Escrito com sucesso!")
    print("*********************")

    spark.stop()
    