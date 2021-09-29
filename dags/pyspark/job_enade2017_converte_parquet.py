from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace

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
            .appName("ENADE2017-Job")\
            .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark
        .read
        .format("csv")
        .options(header='true', inferSchema='true', delimiter=';')
        .load("s3a://dl-landing-zone-539445819060/enade2017/")
    )
    
    print("*********************")
    print("Corrige NT_GER......")
    print("*********************")

    df = (
        df
        .withColumn('NT_GER', regexp_replace('NT_GER', ',', '.').cast('float'))
        .withColumn('NT_FG', regexp_replace('NT_FG', ',', '.').cast('float'))
        .withColumn('NT_CE', regexp_replace('NT_CE', ',', '.').cast('float'))
    )
    
    df.printSchema()

    (df
    .write
    .mode("overwrite")
    .format("parquet")
    .save("s3a://dl-processing-zone-539445819060/enade2017/")
    )

    print("*********************")
    print("Escrito com sucesso!")
    print("*********************")

    spark.stop()