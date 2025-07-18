from pyspark.sql import SparkSession


def get_spark(app_name: str = "SparkApp") -> SparkSession:
    """
    Initialize and return a Spark session.

    """
    spark = (
            SparkSession.builder
            .appName(app_name)
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.2.1")
            .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.my_catalog.type", "hadoop")
            .config("spark.sql.catalog.my_catalog.warehouse", "s3a://gh-lake-neel/datalake/")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            .config("spark.executor.cores", "2")
            .config("spark.executor.instances", "2")
            .getOrCreate()
    )
    return spark