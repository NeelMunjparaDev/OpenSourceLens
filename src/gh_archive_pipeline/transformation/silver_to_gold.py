from gh_archive_pipeline.utils.spark_helper import get_spark, get_spark_local
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark import StorageLevel
from dotenv import load_dotenv
import os


# Load environment variables from .env file
load_dotenv()

ICEBERG_SILVER_TABLE = os.getenv("ICEBERG_SILVER_TABLE")
ICEBERG_GOLD_REPO_HEALTH = os.getenv("ICEBERG_GOLD_REPO_HEALTH")
ICEBERG_GOLD_BRANCH_ACTIVITY = os.getenv("ICEBERG_GOLD_BRANCH_ACTIVITY")
ICEBERG_GOLD_CONTRIBUTOR_CONSISTENCY = os.getenv("ICEBERG_GOLD_CONTRIBUTOR_CONSISTENCY")
ICEBERG_GOLD_TABLES = {
    "repo_health": ICEBERG_GOLD_REPO_HEALTH,
    "branch_activity": ICEBERG_GOLD_BRANCH_ACTIVITY,
    "contributor_consistency": ICEBERG_GOLD_CONTRIBUTOR_CONSISTENCY
}


def get_repo_health_score(spark, df, top_n=50):
    """
    Calculate health score 
    comits + contributors 
    diversity for top N repositories/day.
    """
    df = df.filter(
            ~lower(col("author_name")).contains("bot") &
            ~lower(col("author_name")).contains("github action") &
            ~lower(col("author_email")).contains("action@github.com") &
            ~lower(col("author_email")).contains("github-actions@github.com")
        )
    df = df.withColumn("date", to_date(col("created_at")))
    grouped_df = df.groupBy("repo_name", "date") \
                    .agg(count("*").alias("commit_count"),
                    countDistinct("author_name").alias("contributor_count"))
    max_values = grouped_df.groupBy("date") \
                        .agg(max("commit_count").alias("max_commits"),
                        max("contributor_count").alias("max_contributors"))
    normalized_df = grouped_df.join(max_values, "date") \
                        .withColumn("norm_commits", col("commit_count") / col("max_commits") * 100) \
                        .withColumn("norm_contributors", col("contributor_count") / col("max_contributors") * 100) \
                        .withColumn("health_score", col("norm_commits") * 0.6 + col("norm_contributors") * 0.4) \
                        .filter(col("contributor_count") > 1) \
                        .withColumn("health_flag",when(col("contributor_count") <= 2, "risky")
                                                .when((col("health_score") > 75) & (col("contributor_count") > 5), "healthy")
                                                .otherwise("moderate"))
    window = Window.partitionBy("date").orderBy(col("commit_count").desc())
    result = normalized_df.withColumn("rank", row_number().over(window)) \
                          .filter(col("rank") <= top_n) \
                          .select("repo_name", "date", "rank","max_commits", "commit_count", "contributor_count", 
                                  round(col("norm_commits"),2).alias("normalized_commit_count"), 
                                  round(col("norm_contributors"),2).alias("normalized_contributor_count"), 
                                  round(col("health_score"),2).alias("health_score"), "health_flag") \
                          .orderBy("date", "rank")
    return result

def get_branch_activity_distribution(spark, df, top_n=100):
    """
    Count commits per repo name, ref and day for the top N repositories.
    """
    df = df.filter(
            ~lower(col("author_name")).contains("bot") &
            ~lower(col("author_name")).contains("github action") &
            ~lower(col("author_email")).contains("action@github.com") &
            ~lower(col("author_email")).contains("github-actions@github.com")
        )
    df = df.withColumn("date", to_date(col("created_at")))
    grouped_df = df.groupBy("repo_name", "ref", "date") \
                    .agg(count("*").alias("commit_count"))
    window = Window.partitionBy("date") \
                    .orderBy(col("commit_count").desc())
    top_repos = grouped_df.withColumn("rank", row_number().over(window)) \
                    .filter(col("rank") <= top_n) \
                    .select("repo_name","date")
    branch_df = df.join(top_repos, ["repo_name", "date"]) \
                    .groupBy("repo_name", col("ref").alias("branch"), "date") \
                    .agg(count("*").alias("commit_count")) \
                    .filter(col("commit_count") >= 5)
    window_ntile = Window.partitionBy("date").orderBy(col("commit_count"))
    result = branch_df.withColumn("percentile", ntile(100).over(window_ntile)) \
                    .withColumn("activity_flag",
                        when(col("percentile")> 75, "active")
                    .when(col("percentile").between(25, 75), "moderate")
                    .otherwise("inactive")) \
                    .orderBy("date", "repo_name", col("commit_count").desc())
    return result

def get_contributor_consistency(spark, df, top_n=20, min_active_days=1):
    """
    Calculate contributor consistency for the top N repositories.
    """
    df = df.filter(
            ~lower(col("author_name")).contains("bot") &
            ~lower(col("author_name")).contains("github action") &
            ~lower(col("author_email")).contains("action@github.com") &
            ~lower(col("author_email")).contains("github-actions@github.com")
        )
    df = df.withColumn("date", to_date(col("created_at")))
    repo_rank = df.groupBy("repo_name", "month") \
                .agg(count("*").alias("repo_commit_count"))
    window_repo = Window.partitionBy("month").orderBy(col("repo_commit_count").desc())
    top_repos = repo_rank.withColumn("repo_rank", row_number().over(window_repo)) \
                .filter(col("repo_rank") <= top_n) \
                .select("repo_name", "month")
    result = df.join(top_repos, ["repo_name", "month"])\
               .groupBy("repo_name", col("author_name").alias("contributor"), col("author_email").alias("contributor_email"), "year", "month") \
               .agg(countDistinct("date").alias("active_days")) \
               .filter(col("active_days") >= min_active_days) \
               .withColumn("consistency_level", when(col("active_days") >= 15, "high")
               .when(col("active_days") >= 7, "moderate")
               .otherwise("low")) \
               .orderBy("month")
    return result
    
def write_to_gold_table_y_m_d(df, table_name, create=False):
    """
    Write the transformed DataFrame to the gold table.
    """
    df = df.withColumn("year", year("date")) \
            .withColumn("month", month("date")) \
            .withColumn("day", dayofmonth("date"))
    writer = df.writeTo(table_name).using("iceberg")
    if create:
        writer.partitionedBy("year", "month", "day").createOrReplace()
    else:
        writer.overwritePartitions()

def write_to_gold_table_y_m(df, table_name, create=False):
    """
    Write the transformed DataFrame to the gold table.
    """
    writer = df.writeTo(table_name).using("iceberg")
    if create:
        writer.partitionedBy("year", "month").createOrReplace()
    else:
        writer.overwritePartitions()

def table_exists(spark, table_name):
        return spark.catalog.tableExists(table_name)

TRANSFORM_WRITE_MAP = {
    "repo_health": (get_repo_health_score, write_to_gold_table_y_m_d),
    "branch_activity": (get_branch_activity_distribution, write_to_gold_table_y_m_d),
    "contributor_consistency": (get_contributor_consistency, write_to_gold_table_y_m),
}

def main():
    spark = get_spark_local("Aggregation App")
    df = spark.read.format("iceberg").load(ICEBERG_SILVER_TABLE)
    create = not table_exists(spark,ICEBERG_SILVER_TABLE)
    try:
        for key, table_name in ICEBERG_GOLD_TABLES.items():
            transform_fn, write_fn = TRANSFORM_WRITE_MAP[key]
            write_fn(transform_fn(spark, df), table_name, create)
    finally:
        spark.stop()