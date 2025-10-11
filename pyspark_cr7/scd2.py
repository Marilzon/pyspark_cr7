from pyspark.sql.session import SparkSession

spark = (
    SparkSession.builder
    .config("spark.sql.repl.eagerEval.enabled", True)
    .config("spark.sql.repl.eagerEval.truncate", 0)  # 0 = no truncation
    .getOrCreate()
)

print(spark.version)