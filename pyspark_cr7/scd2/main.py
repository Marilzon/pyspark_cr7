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


# add metadata function
def add_scd2_metadata(dataframe: DataFrame):
    logger.info(
        "Adding SCD2 metadata columns: is_active, start_date, end_date, version"
    )
    return (
        dataframe.withColumn("is_active", lit(1))
        .withColumn("start_date", coalesce(dataframe["created_date"]))
        .withColumn("end_date", lit(""))
        .withColumn("version", lit(1))
    )


def new_data_classifier(current_data: DataFrame, new_data: DataFrame):
    current_data.createOrReplaceTempView("current_data")
    new_data.createOrReplaceTempView("new_data")

    changes_query = """
    SELECT *
    FROM new_data
    EXCEPT
    SELECT * EXCEPT(start_date, end_date, version, is_active)
    FROM current_data
    """

    return spark.sql(changes_query)


def expire_old_register(target_table: str, data_to_update_df: DataFrame, ids: list):
    data_to_update_df.createOrReplaceTempView("data_to_expire")
    join_conditions = " AND ".join([f"target.{id} = source.{id}" for id in ids])

    return spark.sql(
        f"""
        MERGE INTO {target_table} AS target
        USING data_to_expire AS source
        ON {join_conditions} AND target.is_active = 1
        WHEN MATCHED THEN UPDATE SET
            target.is_active = 0,
            target.end_date = CURRENT_DATE()
    """
    )


# animals data csv
logger.info("Reading animals data from CSV file: pyspark_cr7/scd2/data/animals.csv")
current_data_df = (
    spark.read.format("csv")
    .option("header", "true")
    .load("pyspark_cr7/scd2/data/animals.csv")
)

logger.info(f"dataframe loaded with {current_data_df.count()} rows")

# add metadata
current_data_df = add_scd2_metadata(current_data_df)
logger.info("SCD2 metadata added successfully")


# write animals table
current_data_df.write.format("delta").mode("overwrite").saveAsTable("scd_type2_animals")
logger.info("Animals table successfully written to Delta format")


# read animals table
logger.info("Reading animals data from Delta table: scd_type2_animals")
current_data_df = spark.read.table("scd_type2_animals")
current_data_df.show()
current_data_df.printSchema()


# new animals data csv
logger.info(
    "Reading updated animals data from CSV file: pyspark_cr7/scd2/data/updated_animals.csv"
)
new_data_df = (
    spark.read.format("csv")
    .option("header", "true")
    .load("pyspark_cr7/scd2/data/updated_animals.csv")
)

new_data_df.show()
new_data_df.printSchema()


# Classify new data and expire old register
logger.info("Classify new data and expire old register")

data_to_update_df = new_data_classifier(
    current_data=current_data_df, new_data=new_data_df
)

data_to_update_df.show()

expire_old_register("scd_type2_animals", data_to_update_df, ["id"])

# Append new regieters with metadata


def append_new_scd2_data(updated_dataframe: DataFrame, table_name: str, ids: list):

    latest_data_df = spark.read.table(table_name)
    latest_data_df = current_data_df.groupBy(ids).agg(
        spark_max("version").alias("latest_version")
    )

    join_condition = [id for id in ids]

    to_append_records_df = updated_dataframe.join(
        latest_data_df, join_condition, "left"
    )

    to_append_records_df = (
        to_append_records_df.withColumn(
            "version",
            when(col("latest_version").isNull(), lit(1)).otherwise(
                col("latest_version") + 1
            ),
        )
        .withColumn("start_date", (col("created_date")))
        .withColumn("end_date", lit(""))
        .withColumn("is_active", lit(1))
        .drop("latest_version")
    )

    return (
        to_append_records_df.write.format("delta")
        .mode("append")
        .saveAsTable(table_name)
    )


append_new_scd2_data(
    updated_dataframe=data_to_update_df, table_name="scd_type2_animals", ids=["id"]
)


spark.read.table("scd_type2_animals").show()
spark.stop()
