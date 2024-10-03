
from .enums import *

from pyspark import SparkConf, SparkContext

from biocypher._logger import logger

from .node_generator import NodeGenerator
from .edge_generator import EdgeGenerator
from .data_loader import DataLoader

class TargetDiseaseEvidenceAdapter:
    def __init__(
            self,
            datasets: list[TargetDiseaseDataset],
            node_fields: list[TargetNodeField | DiseaseNodeField],
            edge_fields: list[TargetDiseaseEdgeField],
            test_mode: bool = False,
            environment: str = "test"
            
    ):
        self.datasets = datasets
        self.node_fields = node_fields
        self.edge_fields = edge_fields
        self.test_mode = test_mode
        self.test_size = 10

        self.dl = DataLoader(environment)


        if not self.datasets:
            raise ValueError("datasets must be provided")

        if not self.node_fields:
            raise ValueError("node_fields must be provided")

        if not self.edge_fields:
            raise ValueError("edge_fields must be provided")

        if not TargetNodeField.TARGET_GENE_ENSG in self.node_fields:
            raise ValueError("TargetNodeField.TARGET_GENE_ENSG must be provided")

        if not DiseaseNodeField.DISEASE_ACCESSION in self.node_fields:
            raise ValueError("DiseaseNodeField.DISEASE_ACCESSION must be provided")

        if self.test_mode:
            logger.warning("Open Targets adapter: Test mode is enabled. Only processing {self.test_size} rows.")

        # Initialize NodeGenerator
        self.node_generator = NodeGenerator(self.dl.target_df, self.dl.disease_df, self.node_fields, self.test_mode)

        self.edge_generator = EdgeGenerator(self.dl.abod_df, self.dl.aboid_df, self.dl.abdsd_df, self.dl.abdsid_df, self.test_mode)

    def get_nodes(self):
        return self.node_generator.get_nodes()

    # Edge methods would remain as they were, using the EdgeGenerator
    def get_abod_edges(self):
        return self.edge_generator.get_abod_edges()

    def get_aboid_edges(self):
        return self.edge_generator.get_aboid_edges()

    def get_abdsd_edges(self):
        return self.edge_generator.get_abdsd_edges()

    def get_abdsid_edges(self):
        return self.edge_generator.get_abdsid_edges()