from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean

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
    
    print("****************")
    print("* AGREGA NOTAS *")
    print("****************")

    uf_mt = (
        df
        .groupBy("SG_UF_RESIDENCIA")
        .agg(mean(col("NU_NOTA_MT")).alias("med_mt"))
    )

    uf_cn = (
        df
        .groupBy("SG_UF_RESIDENCIA")
        .agg(mean(col("NU_NOTA_CN")).alias("med_cn"))
    )

    uf_ch = (
        df
        .groupBy("SG_UF_RESIDENCIA")
        .agg(mean(col("NU_NOTA_CH")).alias("med_ch"))
    )

    uf_lc = (
        df
        .groupBy("SG_UF_RESIDENCIA")
        .agg(mean(col("NU_NOTA_LC")).alias("med_lc"))
    )

    uf_notas = (
        uf_mt
        .join(uf_cn, on="SG_UF_RESIDENCIA", how="inner")
        .join(uf_ch, on="SG_UF_RESIDENCIA", how="inner")
        .join(uf_lc, on="SG_UF_RESIDENCIA", how="inner")
    )

    (
        uf_notas
        .write
        .mode("overwrite")
        .format("parquet")
        .save("s3a://dl-processing-zone-539445819060/intermediarias/uf_notas")
    )

    print("*********************")
    print("Escrito com sucesso!")
    print("*********************")

    spark.stop()
    