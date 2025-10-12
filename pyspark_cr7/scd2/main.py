import logging
from pyspark.sql.session import SparkSession, DataFrame
from pyspark.sql.functions import (
    lit,
    max as spark_max,
    when,
    col,
)
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
        .withColumn("start_date", dataframe["created_date"])
        .withColumn("end_date", lit("9999-12-31"))
        .withColumn("version", lit(1))  
    )

# animals data csv
logger.info("Reading animals data from CSV file: pyspark_cr7/scd2/data/animals.csv")
animals_df = (
    spark.read.format("csv")
    .option("header", "true")
    .load("pyspark_cr7/scd2/data/animals.csv")
)

logger.info("animals csv dataframe:")
logger.info(f"Animals CSV dataframe loaded with {animals_df.count()} rows")

# add metadata
animals_df = add_scd2_metadata(animals_df)
logger.info("SCD2 metadata added successfully")

# write animals table
animals_df.write.format("delta").mode("overwrite").saveAsTable("scd_type2_animals")
logger.info("Animals table successfully written to Delta format")

# read animals table
logger.info("Reading animals data from Delta table: scd_type2_animals")
scd_type2_animals_df = spark.read.table("scd_type2_animals")
scd_type2_animals_df.show()

# updated animals data csv
logger.info(
    "Reading updated animals data from CSV file: pyspark_cr7/scd2/data/updated_animals.csv"
)
updated_animals_df = (
    spark.read.format("csv")
    .option("header", "true")
    .load("pyspark_cr7/scd2/data/updated_animals.csv")
)

updated_animals_df.show()

# Create temporary views
updated_animals_df.createOrReplaceTempView("updated_animals")
scd_type2_animals_df.createOrReplaceTempView("animals")

# Detectar mudanças CORRETAMENTE
logger.info("Detecting changes between original and updated data")

changes_query = """
    SELECT 
        ua.id,
        ua.animal as new_animal,
        ua.created_date as new_created_date,
        a.animal as old_animal,
        a.created_date as old_created_date,
        a.version as old_version
    FROM updated_animals ua
    LEFT JOIN animals a ON ua.id = a.id AND a.is_active = 1
    WHERE a.id IS NULL  
       OR ua.animal != a.animal 
       OR ua.created_date != a.created_date 
"""

changes_df = spark.sql(changes_query)
changes_df.show()

# Separar em registros para expirar e novos registros
records_to_expire = changes_df.filter(col("old_animal").isNotNull()).select(
    col("id").alias("expire_id")
)

new_and_changed_records = changes_df.select(
    col("id"),
    col("new_animal").alias("animal"),
    col("new_created_date").alias("created_date")
)

records_to_expire.show()
new_and_changed_records.show()

# Expire old records
logger.info("Expiring old records")
if records_to_expire.count() > 0:
    records_to_expire.createOrReplaceTempView("records_to_expire")
    
    expire_result = spark.sql("""
        MERGE INTO scd_type2_animals AS target
        USING records_to_expire AS source
        ON target.id = source.expire_id AND target.is_active = 1
        WHEN MATCHED THEN UPDATE SET
            target.is_active = 0,
            target.end_date = current_date()
    """)
    
    logger.info(f"MERGE completed: {expire_result}")
else:
    logger.info("No records to expire")

# Verificar registros expirados
logger.info("Checking expired records:")
spark.sql("SELECT * FROM scd_type2_animals WHERE is_active = 0").show()

# RELER a tabela após o MERGE para obter as versões atualizadas
scd_type2_animals_df = spark.read.table("scd_type2_animals")
scd_type2_animals_df.createOrReplaceTempView("animals")

# Obter as últimas versões da tabela ATUALIZADA
latest_versions = scd_type2_animals_df.filter(col("is_active").isin([0, 1])) \
    .groupBy("id") \
    .agg(spark_max("version").alias("latest_version"))

latest_versions.show()

# Preparar novos registros
logger.info("Preparing new and changed records")

append_records = (
    new_and_changed_records.alias("ncr")
    .join(latest_versions.alias("lv"), col("ncr.id") == col("lv.id"), "left")
    .withColumn(
        "version",
        when(col("lv.latest_version").isNull(), lit(1))  
        .otherwise(col("lv.latest_version") + 1)
    )
    .withColumn("start_date", col("ncr.created_date"))
    .withColumn("end_date", lit("9999-12-31"))
    .withColumn("is_active", lit(1))
    .select(
        "ncr.id", "ncr.animal", "ncr.created_date", 
        "start_date", "end_date", "version", "is_active"
    )
)

append_records.show()

# Inserir novos registros
logger.info("Inserting new and changed records")
if append_records.count() > 0:
    append_records.write.format("delta").mode("append").saveAsTable("scd_type2_animals")
    logger.info("New records inserted successfully")
else:
    logger.info("No new records to insert")

# Resultado final
logger.info("Final result - all records:")
final_result = spark.sql("""
    SELECT * FROM scd_type2_animals 
    ORDER BY id, version DESC
""")
final_result.show()

logger.info("Final result - active records only:")
active_result = spark.sql("""
    SELECT * FROM scd_type2_animals 
    WHERE is_active = 1
    ORDER BY id
""")
active_result.show()

spark.stop()