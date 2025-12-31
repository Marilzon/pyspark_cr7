import logging
from pyspark.sql.session import SparkSession, DataFrame
from pyspark.sql.types import DateType
from pyspark.sql.functions import lit, max as spark_max, when, col, coalesce
from delta import *

builder = (
    SparkSession.builder.appName("scd2")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()


# logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
logger.info("Spark session created successfully with Delta Lake configuration")
logger.info(f"{spark.version}")

spark.stop()
