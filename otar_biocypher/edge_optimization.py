import time
import hashlib
from tqdm import tqdm
from typing import Optional

from pyspark.sql import SparkSession

from bioregistry.resolve import normalize_curie


def _process_id_and_type(inputId: str, _type: Optional[str] = None):
    """
    Process diseaseId and diseaseType fields from evidence data. Process
    gene (ENSG) ids.

    Args:

        inputId: id of the node.

        _type: type of the node.
    """

    if not inputId:
        return (None, None)

    _id = None

    if _type:
        _id = normalize_curie(f"{_type}:{inputId}")

        return (_id, _type)

    # detect delimiter (either _ or :)
    if "_" in inputId:
        _type = inputId.split("_")[0].lower()

        # special case for OTAR TODO
        if _type == "otar":
            _id = f"otar:{inputId.split('_')[1]}"
        else:
            _id = normalize_curie(inputId, sep="_")

    elif ":" in inputId:
        _type = inputId.split(":")[0].lower()
        _id = normalize_curie(inputId, sep=":")

    if not _id:
        return (None, None)

    return (_id, _type)


class EdgeGenerator:
    def __init__(self):
        self.spark = (
            SparkSession.builder.master("local")  # type: ignore
            .appName("otar_biocypher")
            .config('spark.driver.memory', '4g')
            .config('spark.executor.memory', '4g')
            .master('local[*]')
            .getOrCreate()
        )

        self.abo_df = self.spark.read.parquet("data/pot_files/abo")

    @staticmethod
    def encoding(row):
        return hashlib.md5(str(row).encode()).hexdigest()

    @staticmethod
    def process_abo(row):
        edge_id = hashlib.md5(str(row).encode()).hexdigest()
        disease_id, _ = _process_id_and_type(row['diseaseId'])
        gene_id, _ = _process_id_and_type(row['targetId'], "ensembl")
        properties = {
            'score': row['score'],
            'evidenceCount': row['evidenceCount'],
            'source': 'source',
            'licence': 'licence'
        }
        return (edge_id, gene_id, disease_id, 'abo', properties)

    def get_abo_edges(self):
        # Apply transformation in parallel using map
        self.abo_df = self.abo_df.limit(200000)
        print('count data: ', self.abo_df.count())
        self.abo_df = self.abo_df.repartition(100).cache()
        processed_rdd = self.abo_df.rdd.map(self.process_abo)  # Accessing static method
        results = processed_rdd.collect()
        return results
 