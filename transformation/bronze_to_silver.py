from utils.spark_helper import get_spark
from pyspark.sql.functions import *
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

BRONZE_BASE = os.getenv("BRONZE_BASE")

def load_day(spark, day_path: str):
    """
    Load a day's data from the bronze bucket.
    """
    bronze_path = f"{BRONZE_BASE}{day_path}/*/events.json.gz"
    return spark.read.json(bronze_path, multiLine=True)


def filter_push_events(df):
    """
    Filter the DataFrame to include only push events.
    """
    return df.filter(col("type") == "PushEvent")

def flatten_push_events(df):
    """
    Flatten the push events DataFrame.
    """
    df_commits = df.withColumn("commit", explode(col("payload.commits")))
    flattened_df = df_commits.select(
        col("id").alias("event_id"),
        "created_at",
        col("repo.name").alias("repo_name"),
        col("actor.login").alias("actor_login"),
        col("actor.id").alias("actor_id"),
        col("payload.ref").alias("ref"),
        col("payload.before").alias("before_sha"),
        col("payload.head").alias("head_sha"),
        col("commit.sha").alias("commit_sha"),
        col("commit.message").alias("commit_message"),
        col("commit.author.name").alias("author_name"),
        col("commit.author.email").alias("author_email"),
        col("commit.url").alias("commit_url")
    )
    return flattened_df



if __name__ == "__main__":
    spark = get_spark("BronzeToSilverTransformation")
    day_path = "2025/07/14"  
    df = load_day(spark, day_path)
    push_events_df = filter_push_events(df)
    flattened_df = flatten_push_events(push_events_df)
    flattened_df.select("repo_name","created_at").show()