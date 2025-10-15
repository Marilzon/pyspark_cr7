import logging
from pyspark.sql.session import SparkSession, DataFrame
import pyspark.sql.functions as F
from delta import *

builder = (
    SparkSession.builder.appName("scd2")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark: SparkSession = configure_spark_with_delta_pip(builder).getOrCreate()

# logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
logger.info("Spark session created successfully with Delta Lake configuration")

# functions

def add_landing_date_column(dataframe: DataFrame):
    return dataframe.withColumn("__catalog_insert_date", F.current_date())

def scd1_merge(
    target_table: str,
    new_dataframe: DataFrame,
    ids: list,
    date_deduplicate_cols: list,
):
    new_dataframe.createOrReplaceTempView("new_dataframe")
    join_conditions = " AND ".join([f"target.{id} = source.{id}" for id in ids])
    date_compare_conditions = " AND ".join([f"source.{date} > target.{date}" for date in date_deduplicate_cols])
    
    return spark.sql(
    f"""
        MERGE INTO {target_table} AS target
        USING new_dataframe AS source
        ON {join_conditions}
        WHEN MATCHED AND {date_compare_conditions} THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
    """
    )   
    
# reading first csv
animals_df = spark.read.option("header", True).csv("pyspark_cr7/scd1/data/animals.csv")
logger.info(f"{animals_df} datafreme read with success with {animals_df.count()} lines")

# writing table

animals_df = add_landing_date_column(animals_df)

animals_df.write.format("delta").mode("overwrite").partitionBy("__catalog_insert_date").saveAsTable("animals")
logger.info(f"table created: 'animals'")
spark.read.table("animals").show()

# create new data and add metadata
logger.info(f"Create 'new_animals_df' and add metadata:")
new_animals_df = spark.read.option("header", True).csv("pyspark_cr7/scd1/data/updated_animals.csv")
new_animals_df = add_landing_date_column(new_animals_df)
new_animals_df.show()
 
# perform merge scd1  
logger.info(f"Perform merge scd1 for 'animals'")
scd1_merge(target_table="animals", new_dataframe=new_animals_df, ids=["id"], date_deduplicate_cols=["created_date"])
final_animals_df = spark.read.table("animals")
final_animals_df = final_animals_df.withColumn("id", F.col("id").cast("int"))

final_animals_df.orderBy("id").show()