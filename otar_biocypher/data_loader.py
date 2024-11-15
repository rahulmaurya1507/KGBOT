import pandas as pd
from biocypher._logger import logger
from pyspark.sql import SparkSession


pd.set_option('display.max_rows', None)     # Show all rows
pd.set_option('display.max_colwidth', None) # Prevent truncating column contents
pd.set_option('display.width', 100000) 
pd.set_option('display.max_columns', None)  # Show all columns


class DataLoader:
    def __init__(self, test_size: int = None) -> None:
        """
        Initialize DataLoader with a specific environment.
        
        Args:
            environment (str): The environment for loading data. Can be 'dev' or 'test'.
        """
        self.test_size = test_size
        self.environment = None

        # Set base path based on environment
        if self.test_size:
            self.environment = 'test'
        else:
            self.environment = 'dev'
        
        self.base_path = "data/pot_files" 

        logger.info(f"Creating Spark session for {self.environment} environment.")

        # Create SparkSession
        self.spark = (
            SparkSession.builder.master("local")  # type: ignore
            .appName("otar_biocypher")
            .config('spark.driver.memory', '4g')
            .config('spark.executor.memory', '4g')
            .getOrCreate()
        )

        # Load the data
        #============================= Nodes Data ===============================#
        logger.info('Starting Data Loading!!!')
        target_path = f"{self.base_path}/tp"
        self.target_df = self.spark.read.parquet(target_path)

        disease_path = f"{self.base_path}/diseases"
        self.disease_df = self.spark.read.parquet(disease_path)

        drug_path = f"{self.base_path}/molecule"
        self.drug_df = self.spark.read.parquet(drug_path)

        hpo_path = f"{self.base_path}/hpo"
        self.hpo_df = self.spark.read.parquet(hpo_path)

        #============================= Edges Data ===============================#
        abo_path = f"{self.base_path}/abo"
        self.abo_df = self.spark.read.parquet(abo_path)

        abodid_path = f"{self.base_path}/abodid"
        self.abodid_df = self.spark.read.parquet(abodid_path)

        abds_path = f"{self.base_path}/abds"
        self.abds_df = self.spark.read.parquet(abds_path)

        abdsdid_path = f"{self.base_path}/abdsdid"
        self.abdsdid_df = self.spark.read.parquet(abdsdid_path)

        abdt_path = f"{self.base_path}/abdt"
        self.abdt_df = self.spark.read.parquet(abdt_path)

        abdtdid_path = f"{self.base_path}/abdtdid"
        self.abdtdid_df = self.spark.read.parquet(abdtdid_path)

        molecular_interactions_path = f"{self.base_path}/interaction"
        self.molecular_interactions_df = self.spark.read.parquet(molecular_interactions_path)

        dmoa_path = f"{self.base_path}/dmoa"
        self.dmoa_df = self.spark.read.parquet(dmoa_path)

        indications_path = f"{self.base_path}/indications"
        self.indications_df = self.spark.read.parquet(indications_path)

        disease2phenotype_path = f"{self.base_path}/diseaseToPhenotype"
        self.disease2phenotype_df = self.spark.read.parquet(disease2phenotype_path)
        
        logger.info('Data Loading Completed!!!')

        # Apply test size limit if specified
        if self.test_size:
            #============================= Nodes Data ===============================#
            self.target_df = self.target_df.limit(self.test_size)
            self.disease_df = self.disease_df.limit(self.test_size)
            self.drug_df = self.drug_df.limit(self.test_size)
            self.hpo_df = self.hpo_df.limit(self.test_size)

            #============================= Edges Data ===============================#
            self.abo_df = self.abo_df.limit(self.test_size)
            self.abodid_df = self.abodid_df.limit(self.test_size)
            self.abds_df = self.abds_df.limit(self.test_size)
            self.abdsdid_df = self.abdsdid_df.limit(self.test_size)
            self.abdt_df = self.abdt_df.limit(self.test_size)
            self.abdtdid_df = self.abdtdid_df.limit(self.test_size)
            self.molecular_interactions_df = self.molecular_interactions_df.limit(self.test_size)
            self.dmoa_df = self.dmoa_df.limit(self.test_size)
            self.indications_df = self.indications_df.limit(self.test_size)
            self.disease2phenotype_df = self.disease2phenotype_df.limit(self.test_size)
