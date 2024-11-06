from biocypher import BioCypher

# VSCode does not add the root directory to the path (by default?). Not sure why
# this works sometimes and not others. This is a workaround.
import sys

sys.path.append("")

from otar_biocypher.ota import (
    TargetDiseaseEvidenceAdapter,
    TargetDiseaseDataset,
    TargetNodeField,
    DiseaseNodeField,
    TargetDiseaseEdgeField,
)

target_disease_datasets = [
    TargetDiseaseDataset.CANCER_BIOMARKERS,
    TargetDiseaseDataset.CANCER_GENE_CENSUS,
    TargetDiseaseDataset.CHEMBL,
    TargetDiseaseDataset.CLINGEN,
    TargetDiseaseDataset.CRISPR,
    TargetDiseaseDataset.EUROPE_PMC,
    TargetDiseaseDataset.EVA,
    TargetDiseaseDataset.EVA_SOMATIC,
    TargetDiseaseDataset.EXPRESSION_ATLAS,
    TargetDiseaseDataset.GENOMICS_ENGLAND,
    TargetDiseaseDataset.GENE_BURDEN,
    TargetDiseaseDataset.GENE2PHENOTYPE,
    TargetDiseaseDataset.IMPC,
    TargetDiseaseDataset.INTOGEN,
    TargetDiseaseDataset.ORPHANET,
    TargetDiseaseDataset.OT_GENETICS_PORTAL,
    TargetDiseaseDataset.PROGENY,
    TargetDiseaseDataset.REACTOME,
    TargetDiseaseDataset.SLAP_ENRICH,
    TargetDiseaseDataset.SYSBIO,
    TargetDiseaseDataset.UNIPROT_VARIANTS,
    TargetDiseaseDataset.UNIPROT_LITERATURE,
]

target_disease_node_fields = [
    # mandatory fields
    TargetNodeField.TARGET_GENE_ENSG,
    DiseaseNodeField.DISEASE_ACCESSION,
    # optional target (gene) fields
    TargetNodeField.TARGET_GENE_SYMBOL,
    TargetNodeField.TARGET_GENE_BIOTYPE,
    TargetNodeField.TARGET_NAME,
    TargetNodeField.TARGET_IS_IN_MEMBRANE,
    TargetNodeField.TARGET_IS_SECRETED,
    TargetNodeField.TARGET_HAS_SAFETY_EVENT,
    TargetNodeField.TARGET_HAS_POCKET,
    TargetNodeField.TARGET_HAS_LIGAND,
    TargetNodeField.TARGET_HAS_SMALL_MOLECULE_BINDER,
    TargetNodeField.TARGET_GENETIC_CONSTRAINT,
    TargetNodeField.TARGET_PARALOG_MAX_IDENTITY_PERCENTAGE,
    TargetNodeField.TARGET_MOUSE_ORTHOLOG_MAX_IDENTITY_PERCENTAGE,
    TargetNodeField.TARGET_IS_CANCER_DRIVER_GENE,
    TargetNodeField.TARGET_HAS_TEP,
    TargetNodeField.TARGET_MOUSE_KO_SCORE,
    TargetNodeField.TARGET_HAS_HIGH_QUALITY_CHEMICAL_PROBES,
    TargetNodeField.TARGET_MAX_CLINICAL_TRIAL_PHASE,
    TargetNodeField.TARGET_TISSUE_SPECIFICITY,
    TargetNodeField.TARGET_TISSUE_DISTRIBUTION,


    # optional disease fields
    DiseaseNodeField.DISEASE_CODE,
    DiseaseNodeField.DISEASE_NAME,
    DiseaseNodeField.DISEASE_DESCRIPTION,
]

target_disease_edge_fields = [
    # mandatory fields
    TargetDiseaseEdgeField.INTERACTION_ACCESSION,
    TargetDiseaseEdgeField.TARGET_GENE_ENSG,
    TargetDiseaseEdgeField.DISEASE_ACCESSION,
    TargetDiseaseEdgeField.TYPE,
    TargetDiseaseEdgeField.SOURCE,
    # optional fields
    TargetDiseaseEdgeField.SCORE,
    TargetDiseaseEdgeField.LITERATURE,
]


def main():
    """
    Main function running the import using BioCypher and the adapter.
    """

    # Start BioCypher
    bc = BioCypher(
        biocypher_config_path="config/biocypher_config.yaml",
    )

    # Check the schema
    bc.show_ontology_structure()

    # Open Targets
    target_disease_adapter = TargetDiseaseEvidenceAdapter(
        datasets=target_disease_datasets,
        node_fields=target_disease_node_fields,
        edge_fields=target_disease_edge_fields,
        test_size=10000
    )

    # Write nodes
    bc.write_nodes(target_disease_adapter.get_nodes())

    bc.write_edges(target_disease_adapter.get_abo_edges())
    bc.write_edges(target_disease_adapter.get_abodid_edges())

    bc.write_edges(target_disease_adapter.get_abds_edges())
    bc.write_edges(target_disease_adapter.get_abdsdid_edges())
    
    bc.write_edges(target_disease_adapter.get_abdt_edges())
    bc.write_edges(target_disease_adapter.get_abdtdid_edges())

    # # Post import functions
    bc.write_import_call()
    bc.summary()

if __name__ == "__main__":
    main()
    

# /var/lib/neo4j/import.report