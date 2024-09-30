import pandas as pd

from biocypher._logger import logger

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


class DataLoader:
    def __init__(self) -> None:


        logger.info("Creating Spark session.")

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

        target_path = "data/ot_files/targets"
        self.target_df = self.spark.read.parquet(target_path)
        self.target_df = self.target_df.withColumn("name", col("approvedSymbol"))
        
        disease_path = "data/ot_files/diseases"
        self.disease_df = self.spark.read.parquet(disease_path)

        abod_path = 'data/ot_files/associationByOverallDirect'
        # self.abod_df = self.spark.read.parquet(abod_path)
        self.abod_df = pd.read_parquet(abod_path)

        aboid_path = 'data/ot_files/associationByOverallIndirect'
        # self.abod_df = self.spark.read.parquet(abod_path)
        self.aboid_df = pd.read_parquet(aboid_path)

        abdsd_path = 'data/ot_files/associationByDatasourceDirect'
        # self.abod_df = self.spark.read.parquet(abod_path)
        self.abdsd_df = pd.read_parquet(abdsd_path)

        abdsid_path = 'data/ot_files/associationByDatasourceIndirect'
        # self.abod_df = self.spark.read.parquet(abod_path)
        self.abdsid_df = pd.read_parquet(abdsid_path)