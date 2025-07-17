from pyspark.sql import SparkSession


def get_spark(app_name: str = "SparkApp") -> SparkSession:
    """
    Initialize and return a Spark session.

    """
    spark = (
            SparkSession.builder
            .appName(app_name)
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
            .getOrCreate()
    )
    return spark