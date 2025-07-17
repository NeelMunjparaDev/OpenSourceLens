from utils.spark_helper import get_spark
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


if __name__ == "__main__":
    spark = get_spark("BronzeToSilverTransformation")
    day_path = "2025/07/14"  
    df = load_day(spark, day_path)
    df.select("payload").show()  