from pyspark.sql import SparkSession

# Create SparkSession
spark = (
    SparkSession.builder.master("local")  # type: ignore
    .appName("otar_biocypher")
    .config('spark.driver.memory', '4g')
    .config('spark.executor.memory', '4g')
    .master('local[*]')
    .getOrCreate()
)