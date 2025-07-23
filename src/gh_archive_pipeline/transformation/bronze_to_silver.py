from gh_archive_pipeline.utils.spark_helper import get_spark, get_spark_local
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

BRONZE_BASE = os.getenv("BRONZE_BASE")
ICEBERG_SILVER_TABLE = os.getenv("ICEBERG_SILVER_TABLE")

schema_for_push_event = StructType([
                StructField("id", StringType(), False),
                StructField("type", StringType(), True),
                StructField("actor", StructType([
                    StructField("id", LongType(), True),
                    StructField("login", StringType(), True),
                ]), True),
                StructField("repo", StructType([
                    StructField("id", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("url", StringType(), True),
                ]), True),
                StructField("payload", StructType([
                    StructField("ref", StringType(), True),
                    StructField("head", StringType(), True),
                    StructField("before", StringType(), True),
                    StructField("commits", ArrayType(StructType([
                        StructField("sha", StringType(), True),
                        StructField("author", StructType([
                            StructField("name", StringType(), True),
                            StructField("email", StringType(), True),
                        ]), True),
                        StructField("message", StringType(), True),
                        StructField("url", StringType(), True),
                    ])), True),
                ]), True),
                StructField("created_at", StringType(), True)
            ]) 

def load_day(spark, schema, day_path: str):
    """
    Load a day's data from the bronze bucket.
    """
    bronze_path = f"{BRONZE_BASE}{day_path}/*/data.json.gz"
    return spark.read.schema(schema).json(bronze_path)

def load_local(spark,schema):
    """
    Load a day's data from the bronze bucket.
    """
    path = os.getenv("LOCAL_DB_PATH")
    return spark.read.option("recursiveFileLookup", "true").schema(schema).json(path)


def filter_push_events(df: DataFrame) -> DataFrame:
    """
    Filter the DataFrame to include only push events.
    """
    return df.filter(col("type") == "PushEvent")

def flatten_push_events(df: DataFrame) -> DataFrame:
    """
    Flatten the push events DataFrame.
    """
    df_commits = df.withColumn("commit", explode(col("payload.commits")))
    flattened_df = df_commits.select(
        col("id").alias("event_id"),
        "created_at",
        col("actor.id").alias("actor_id"),
        col("actor.login").alias("actor_login"),
        col("repo.name").alias("repo_name"),
        col("payload.ref").alias("ref"),
        col("payload.head").alias("head_sha"),
        col("payload.before").alias("before_sha"),
        col("commit.sha").alias("commit_sha"),
        col("commit.author.email").alias("author_email"),
        col("commit.author.name").alias("author_name"),
        col("commit.message").alias("commit_message"),
        col("commit.url").alias("commit_url")
    )
    return flattened_df

def clean_missing_values(df: DataFrame) -> DataFrame:
    """
    Clean missing values in the DataFrame.
    """
    df = df.dropna(subset=["event_id", "created_at", "commit_sha"])
    df = df.fillna({
        "author_email": "unknown",
        "author_name": "unknown",
        "commit_message": "No message",
        "commit_url": "No URL",
        "ref": "unknown",
        "before_sha": "unknown",
        "head_sha": "unknown"
    })
    return df

def normalized_data(df:DataFrame) -> DataFrame:
    """
    Handeling Schema changes and normalizing data.
    """
    df = df.withColumn("created_at", to_timestamp(col("created_at"), "yyyy-MM-dd'T'HH:mm:ssX")) \
              .withColumn("event_id", col("event_id").cast("long")) \
              .withColumn("ref", regexp_replace(col("ref"), "^refs/heads/", "")) \
              .withColumn("author_email", lower(trim(col("author_email")))) \
              .withColumn("author_name", trim(col("author_name"))) \
              .withColumn("actor_login", trim(col("actor_login"))) \
              .withColumn("repo_name", trim(col("repo_name"))) \
              .withColumn("commit_message", trim(col("commit_message")))
    return df

def derive_columns(df: DataFrame) -> DataFrame:
    """
    Added derived columns to the DataFrame.
    """
    df = df.withColumn("commit_message_length", length(col("commit_message"))) \
           .withColumn("repo_owner", split(col("repo_name"), "/").getItem(0)) \
           .withColumn("year", year("created_at")) \
           .withColumn("month", month("created_at")) \
           .withColumn("day", dayofmonth("created_at"))
    return df

def deduplicate_data(df: DataFrame) -> DataFrame:
    """
    Deduplicate the DataFrame based on event_id and commit_sha.
    """
    return df.dropDuplicates(["event_id", "commit_sha"])

def transform_bronze_to_silver(df) -> DataFrame:
    """
    Applies all transformations steps 
    from raw bronze data to silver data.
    """
    push_events_df = filter_push_events(df)
    flattened_df = flatten_push_events(push_events_df)
    cleaned_df = clean_missing_values(flattened_df)
    normalized_df = normalized_data(cleaned_df)
    derived_df = derive_columns(normalized_df)
    final_df = deduplicate_data(derived_df)
    return final_df

def write_to_silver_table(df: DataFrame, table_name: str, create: bool = False):
    """
    Write the transformed DataFrame to the silver table.
    """
    writer = df.writeTo(table_name).using("iceberg")
    if create:
        writer.partitionedBy("year", "month", "day").createOrReplace()
    else:
        writer.append()

def table_exists(spark, table_name):
        return spark.catalog.tableExists(table_name)

def main():
    # spark = get_spark("BronzeToSilverTransformation")
    spark_local = get_spark_local("BronzeToSilverTransformationLocal")
    create = not table_exists(spark_local,"my_catalog.github_commits.silver")
    df = load_local(spark_local, schema_for_push_event)
    df = transform_bronze_to_silver(df)
    df.show()
    write_to_silver_table(df, "my_catalog.github_commits.silver", create)