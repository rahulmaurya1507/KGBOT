from pyspark.sql import SparkSession


# Create SparkSession with additional string truncation configuration
spark = (
    SparkSession.builder
    .appName("otar_biocypher")
    .config('spark.driver.memory', '4g')  # Set memory for the driver
    .config('spark.executor.memory', '4g')  # Set memory for the executor
    .config('spark.sql.debug.maxToStringFields', 10000)  # Increase the max fields for query plans
    .master("local[*]")  # Use all available cores for local execution
    .getOrCreate()
)
