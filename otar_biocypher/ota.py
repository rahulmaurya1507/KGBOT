
from .enums import *

from biocypher._logger import logger

from .node_generator import NodeGenerator
from .edge_generator import EdgeGenerator
from .data_loader import DataLoader

class TargetDiseaseEvidenceAdapter:
    def __init__(
            self,
            datasets: list[TargetDiseaseDataset],
            node_fields: list[TargetNodeField | DiseaseNodeField | DrugNodeField | HPONodeField],
            test_size: int = None
            
    ):
        self.datasets = datasets
        self.node_fields = node_fields
        self.test_size = test_size

        if not self.datasets:
            raise ValueError("datasets must be provided")

        if not self.node_fields:
            raise ValueError("node_fields must be provided")

        if not TargetNodeField.TARGET_GENE_ENSG in self.node_fields:
            raise ValueError("TargetNodeField.TARGET_GENE_ENSG must be provided")

        if not DiseaseNodeField.DISEASE_ACCESSION in self.node_fields:
            raise ValueError("DiseaseNodeField.DISEASE_ACCESSION must be provided")
        
        if not DrugNodeField.DRUG_ACCESSION in self.node_fields:    
            raise ValueError("DrugNodeField.DRUG_ACCESSION must be provided")

        if self.test_size:
            logger.warning("Open Targets adapter: Test mode is enabled. Only processing {self.test_size} rows.")

        # Initialize DataLoader
        self.dl = DataLoader(
            test_size=self.test_size
        )

        # Initialize NodeGenerator
        self.node_generator = NodeGenerator(
            self.dl.target_df,
            self.dl.disease_df,
            self.dl.drug_df,
            self.dl.hpo_df,
            self.node_fields
        )

        # Initialize EdgeGenerator
        self.edge_generator = EdgeGenerator(
            self.dl.abo_df,
            self.dl.abodid_df,
            self.dl.abds_df,
            self.dl.abdsdid_df,
            self.dl.abdt_df,
            self.dl.abdtdid_df,
            self.dl.dmoa_df,
            self.dl.indications_df,
            self.dl.molecular_interactions_df,
            self.dl.disease2phenotype_df
        )

    def get_nodes(self):
        return self.node_generator.get_nodes()

    # Edge methods would remain as they were, using the EdgeGenerator
    def get_abo_edges(self):
        return self.edge_generator.get_abo_edges()

    def get_abodid_edges(self):
        return self.edge_generator.get_abodid_edges()

    def get_abds_edges(self):
        return self.edge_generator.get_abds_edges()

    def get_abdsdid_edges(self):
        return self.edge_generator.get_abdsdid_edges()
    
    def get_abdt_edges(self):
        return self.edge_generator.get_abdt_edges()

    def get_abdtdid_edges(self):
        return self.edge_generator.get_abdtdid_edges()
    
    def get_dmoa_edges(self):
        return self.edge_generator.get_dmoa_edges()
    
    def get_indication_edges(self):
        return self.edge_generator.get_indication_edges()
    
    def get_molecular_interactions_edges(self):
        return self.edge_generator.get_molecular_interactions_edges()
    
    def get_disease2phenotype_edges(self):
        return self.edge_generator.get_disease2phenotype_edges()