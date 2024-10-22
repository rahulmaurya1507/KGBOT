import pandas as pd
from biocypher._logger import logger
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


class DataLoader:
    def __init__(self, environment: str, test_size: int = None) -> None:
        """
        Initialize DataLoader with a specific environment.
        
        Args:
            environment (str): The environment for loading data. Can be 'dev' or 'test'.
        """
        self.test_size = test_size

        # Set base path based on environment
        if environment == "dev":
            self.base_path = "data/pot_files"
        elif environment == "test":
            self.base_path = "data/test_data"
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
        target_path = f"{self.base_path}/targets"
        self.target_df = self.spark.read.parquet(target_path)
        self.target_df = self.target_df.withColumn("name", col("approvedSymbol"))

        disease_path = f"{self.base_path}/diseases"
        self.disease_df = self.spark.read.parquet(disease_path)

        abo_path = f"{self.base_path}/abo"
        self.abo_df = pd.read_parquet(abo_path)

        abodid_path = f"{self.base_path}/abodid"
        self.abodid_df = pd.read_parquet(abodid_path)

        abds_path = f"{self.base_path}/abds"
        self.abds_df = pd.read_parquet(abds_path)

        abdsdid_path = f"{self.base_path}/abdsdid"
        self.abdsdid_df = pd.read_parquet(abdsdid_path)

        abdt_path = f"{self.base_path}/abdt.parquet"
        self.abdt_df = pd.read_parquet(abdt_path)

        abdtdid_path = f"{self.base_path}/abdtdid"
        self.abdtdid_df = pd.read_parquet(abdtdid_path)

        if self.test_size:
            self.target_df = self.target_df.limit(self.test_size)
            self.disease_df = self.disease_df.limit(self.test_size)

            self.abo_df = self.abo_df.head(self.test_size)
            self.abodid_df = self.abodid_df.head(self.test_size)

            self.abds_df = self.abds_df.head(self.test_size)
            self.abdsdid_df = self.abdsdid_df.head(self.test_size)
            
            self.abdt_df = self.abdt_df.head(self.test_size)
            self.abdtdid_df = self.abdtdid_df.head(self.test_size)
