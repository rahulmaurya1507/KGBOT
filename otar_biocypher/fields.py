import sys

# sys.path.append("")


from .ota import (
    TargetDiseaseDataset,
    TargetNodeField,
    DiseaseNodeField,
    DrugNodeField,
    HPONodeField,
    ReactomeNodeField
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

node_fields = [
    # mandatory fields
    TargetNodeField.TARGET_GENE_ENSG,
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
    TargetNodeField.TARGET_TISSUES,
    # mandatory disease fields
    DiseaseNodeField.DISEASE_ACCESSION,
    # optional disease fields
    DiseaseNodeField.DISEASE_CODE,
    DiseaseNodeField.DISEASE_NAME,
    DiseaseNodeField.DISEASE_DESCRIPTION,
    # mandatory drug fields
    DrugNodeField.DRUG_ACCESSION,
    # optional drug fields
    DrugNodeField.DRUG_CANONICAL_SMILES,
    DrugNodeField.DRUG_INCHIKEY,
    DrugNodeField.DRUG_TYPE,
    DrugNodeField.DRUG_BLACK_BOX_WARNING,
    DrugNodeField.DRUG_NAME,
    DrugNodeField.DRUG_YEAR_OF_FIRST_APPROVAL,
    DrugNodeField.DRUG_MAXIMUM_CLINICAL_TRIAL_PHASE,
    DrugNodeField.DRUG_PARENT_ID,
    DrugNodeField.DRUG_HAS_BEEN_WITHDRAWN,
    DrugNodeField.DRUG_IS_APPROVED,
    DrugNodeField.DRUG_TRADE_NAMES,
    DrugNodeField.DRUG_SYNONYMS,
    DrugNodeField.DRUG_CROSS_REFERENCES,
    DrugNodeField.DRUG_CHILD_CHEMBL_IDS,
    DrugNodeField.DRUG_LINKED_DISEASES,
    DrugNodeField.DRUG_LINKED_TARGETS,
    DrugNodeField.DRUG_DESCRIPTION,
    DrugNodeField.DRUG_INDICATION_COUNT_IN_DISEASE,
    DrugNodeField.DRUG_REACTIONS,
    # mandatory hpo fields
    HPONodeField.HPO_ACCESSION,
    # optional hpo fields
    HPONodeField.HPO_NAME,
    HPONodeField.HPO_DESCRIPTION,
    HPONodeField.HPO_DB_XREFS,
    HPONodeField.HPO_NAMESPACE,
    HPONodeField.HPO_OBSOLETE_TERMS,
    HPONodeField.HPO_PARENTS,
    # mandatory reactome fields
    ReactomeNodeField.REACTOME_ACCESSION,
    # optional reactome fields
    ReactomeNodeField.REACTOME_NAME,
    ReactomeNodeField.REACTOME_LABEL,
    ReactomeNodeField.REACTOME_ANCESTORS,
    ReactomeNodeField.REACTOME_DESCENDANTS,
    ReactomeNodeField.REACTOME_CHILDREN,
    ReactomeNodeField.REACTOME_PARENT,
]
