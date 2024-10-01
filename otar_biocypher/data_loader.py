import pandas as pd
from biocypher._logger import logger
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


class DataLoader:
    def __init__(self, environment: str) -> None:
        """
        Initialize DataLoader with a specific environment.
        
        Args:
            environment (str): The environment for loading data. Can be 'dev' or 'test'.
        """
        # Set base path based on environment
        if environment == "dev":
            base_path = "data/ot_files"
        elif environment == "test":
            base_path = "data/test_data"
        else:
            raise ValueError(f"Unknown environment: {environment}")

        logger.info(f"Creating Spark session for {environment} environment.")

        # Set up Spark context
        conf = (
            SparkConf()
            .setAppName("otar_biocypher")
            .setMaster("local")
            .set("spark.driver.memory", "4g")
            .set("spark.executor.memory", "4g")
        )
        self.sc = SparkContext(conf=conf)

        # Create SparkSession
        self.spark = (
            SparkSession.builder.master("local")  # type: ignore
            .appName("otar_biocypher")
            .getOrCreate()
        )

        # Load the data
        target_path = f"{base_path}/targets"
        self.target_df = self.spark.read.parquet(target_path)
        self.target_df = self.target_df.withColumn("name", col("approvedSymbol"))

        disease_path = f"{base_path}/diseases"
        self.disease_df = self.spark.read.parquet(disease_path)

        abod_path = f"{base_path}/associationByOverallDirect"
        self.abod_df = pd.read_parquet(abod_path)

        aboid_path = f"{base_path}/associationByOverallIndirect"
        self.aboid_df = pd.read_parquet(aboid_path)

        abdsd_path = f"{base_path}/associationByDatasourceDirect"
        self.abdsd_df = pd.read_parquet(abdsd_path)
        self.abdsd_df['name'] = self.abdsd_df['diseaseId'] + '-' + self.abdsd_df['targetId']

        abdsid_path = f"{base_path}/associationByDatasourceIndirect"
        self.abdsid_df = pd.read_parquet(abdsid_path)

