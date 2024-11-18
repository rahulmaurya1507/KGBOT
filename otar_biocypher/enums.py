from enum import Enum


class TargetDiseaseDataset(Enum):
    """
    Enum of all the datasets used in the target-disease evidence pipeline.
    Values are the spellings used in the Open Targets parquet files.
    """

    CANCER_BIOMARKERS = "cancer_biomarkers"
    CANCER_GENE_CENSUS = "cancer_gene_census"
    CHEMBL = "chembl"
    CLINGEN = "clingen"
    CRISPR = "crispr"
    EUROPE_PMC = "europepmc"
    EVA = "eva"
    EVA_SOMATIC = "eva_somatic"
    EXPRESSION_ATLAS = "expression_atlas"
    GENOMICS_ENGLAND = "genomics_england"
    GENE_BURDEN = "gene_burden"
    GENE2PHENOTYPE = "gene2phenotype"
    IMPC = "impc"
    INTOGEN = "intogen"
    ORPHANET = "orphanet"
    OT_GENETICS_PORTAL = "ot_genetics_portal"
    PROGENY = "progeny"
    REACTOME = "reactome"
    SLAP_ENRICH = "slapenrich"
    SYSBIO = "sysbio"
    UNIPROT_VARIANTS = "uniprot_variants"
    UNIPROT_LITERATURE = "uniprot_literature"


_licences = {
    "cancer_biomarkers": "NA",  # TODO
    "cancer_gene_census": "Commercial use for Open Targets",
    "chembl": "CC BY-SA 3.0",
    "clingen": "CC0 1.0",
    "crispr": "NA",  # TODO
    "europepmc": "CC BY-NC 4.0",  # can be open access, CC0, CC BY, or CC BY-NC
    "eva": "EMBL-EBI terms of use",
    "eva_somatic": "EMBL-EBI terms of use",
    "expression_atlas": "CC BY 4.0",
    "genomics_england": "Commercial use for Open Targets",
    "gene_burden": "NA",  # TODO
    "gene2phenotype": "EMBL-EBI terms of use",
    "impc": "NA",  # TODO
    "intogen": "CC0 1.0",
    "orphanet": "CC BY 4.0",
    "ot_genetics_portal": "EMBL-EBI terms of use",
    "progeny": "Apache 2.0",
    "reactome": "CC BY 4.0",
    "slapenrich": "MIT",
    "sysbio": "NA",  # TODO
    "uniprot_variants": "CC BY 4.0",
    "uniprot_literature": "CC BY 4.0",
}


class TargetNodeField(Enum):
    """
    Enum of all the fields in the target dataset. Values are the spellings used
    in the Open Targets parquet files.
    """

    # mandatory fields
    TARGET_GENE_ENSG = "id"
    _PRIMARY_ID = TARGET_GENE_ENSG

    # optional fields
    TARGET_GENE_SYMBOL = "approvedSymbol"
    TARGET_GENE_BIOTYPE = "biotype"
    TARGET_TRANSCRIPT_IDS = "transcriptIds"
    TARGET_CANONICAL_TRANSCRIPT = "canonicalTranscript"
    TARGET_CANONICAL_EXONS = "canonicalExons"
    TARGET_GENOMIC_LOCATIONS = "genomicLocation"
    TARGET_ALTERNATIVE_GENES = "alternativeGenes"
    TARGET_APPROVED_NAME = "approvedName"
    TARGET_GENE_ONTOLOGY_ANNOTATIONS = "go"
    TARGET_HALLMARKS = "hallmarks"
    TARGET_ALL_SYNONYMS = "synonyms"
    TARGET_GENE_SYMBOL_SYNONYMS = "symbolSynonyms"
    TARGET_NAME_SYNONYMS = "nameSynonyms"
    TARGET_FUNCTIONAL_DESCRIPTIONS = "functionDescriptions"
    TARGET_SUBCELLULAR_LOCATIONS = "subcellularLocations"
    TARGET_CLASS = "targetClass"
    TARGET_OBSOLETE_GENE_SYMBOLS = "obsoleteSymbols"
    TARGET_OBSOLETE_GENE_NAMES = "obsoleteNames"
    TARGET_CONSTRAINT = "constraint"
    TARGET_TEP = "tep"
    TARGET_PROTEIN_IDS = "proteinIds"
    TARGET_DATABASE_XREFS = "dbXrefs"
    TARGET_CHEMICAL_PROBES = "chemicalProbes"
    TARGET_HOMOLOGUES = "homologues"
    TARGET_TRACTABILITY = "tractability"
    TARGET_SAFETY_LIABILITIES = "safetyLiabilities"
    TARGET_PATHWAYS = "pathways"
    TARGET_NAME = 'name'

    TARGET_IS_IN_MEMBRANE = 'isInMembrane'
    TARGET_IS_SECRETED = 'isSecreted'
    TARGET_HAS_SAFETY_EVENT = 'hasSafetyEvent'
    TARGET_HAS_POCKET = 'hasPocket'
    TARGET_HAS_LIGAND = 'hasLigand'
    TARGET_HAS_SMALL_MOLECULE_BINDER = 'hasSmallMoleculeBinder'
    TARGET_GENETIC_CONSTRAINT = 'geneticConstraint'
    TARGET_PARALOG_MAX_IDENTITY_PERCENTAGE = 'paralogMaxIdentityPercentage'
    TARGET_MOUSE_ORTHOLOG_MAX_IDENTITY_PERCENTAGE = 'mouseOrthologMaxIdentityPercentage'
    TARGET_IS_CANCER_DRIVER_GENE = 'isCancerDriverGene'
    TARGET_HAS_TEP = 'hasTEP'
    TARGET_MOUSE_KO_SCORE = 'mouseKOScore'
    TARGET_HAS_HIGH_QUALITY_CHEMICAL_PROBES = 'hasHighQualityChemicalProbes'
    TARGET_MAX_CLINICAL_TRIAL_PHASE = 'maxClinicalTrialPhase'
    TARGET_TISSUE_SPECIFICITY = 'tissueSpecificity'
    TARGET_TISSUE_DISTRIBUTION = 'tissueDistribution'

    TARGET_TISSUES = 'tissues'


class DiseaseNodeField(Enum):
    """
    Enum of all the fields in the disease dataset. Values are the spellings used
    in the Open Targets parquet files.
    """

    # mandatory fields
    DISEASE_ACCESSION = "id"
    _PRIMARY_ID = DISEASE_ACCESSION

    # optional fields
    DISEASE_CODE = "code"
    DISEASE_DATABASE_XREFS = "dbXRefs"
    DISEASE_DESCRIPTION = "description"
    DISEASE_NAME = "name"
    DISEASE_DIRECT_LOCATION_IDS = "directLocationIds"
    DISEASE_OBSOLETE_TERMS = "obsoleteTerms"
    DISEASE_PARENTS = "parents"
    DISEASE_SKO = "sko"
    DISEASE_SYNONYMS = "synonyms"
    DISEASE_ANCESTORS = "ancestors"
    DISEASE_DESCENDANTS = "descendants"
    DISEASE_CHILDREN = "children"
    DISEASE_THERAPEUTIC_AREAS = "therapeuticAreas"
    DISEASE_INDIRECT_LOCATION_IDS = "indirectLocationIds"
    DISEASE_ONTOLOGY = "ontology"


class DrugNodeField(Enum):
    """
    Enum of all the fields in the drug dataset. Values are the spellings used
    in the Open Targets parquet files.
    """

    DRUG_ACCESSION = "id"
    _PRIMARY_ID = DRUG_ACCESSION
    DRUG_CANONICAL_SMILES = "canonicalSmiles"
    DRUG_INCHIKEY = "inchiKey"
    DRUG_TYPE = "drugType"
    DRUG_BLACK_BOX_WARNING = "blackBoxWarning"
    DRUG_NAME = "name"
    DRUG_YEAR_OF_FIRST_APPROVAL = "yearOfFirstApproval"
    DRUG_MAXIMUM_CLINICAL_TRIAL_PHASE = "maximumClinicalTrialPhase"
    DRUG_PARENT_ID = "parentId"
    DRUG_HAS_BEEN_WITHDRAWN = "hasBeenWithdrawn"
    DRUG_IS_APPROVED = "isApproved"
    DRUG_TRADE_NAMES = "tradeNames"
    DRUG_SYNONYMS = "synonyms"
    DRUG_CROSS_REFERENCES = "crossReferences"
    DRUG_CHILD_CHEMBL_IDS = "childChemblIds"
    DRUG_LINKED_DISEASES = "linkedDiseases"
    DRUG_LINKED_TARGETS = "linkedTargets"
    DRUG_DESCRIPTION = "description"
    DRUG_INDICATION_COUNT_IN_DISEASE = 'IndicationCountInDisease'
    DRUG_REACTIONS = 'reactions'

class HPONodeField(Enum):
    """
    Enum of all the fields in the hpo dataset. Values are the spellings used
    in the Open Targets parquet files.
    """

    HPO_ACCESSION = "id"
    _PRIMARY_ID = HPO_ACCESSION

    HPO_DB_XREFS = "dbXRefs"
    HPO_DESCRIPTION = "description"
    HPO_NAME = "name"
    HPO_NAMESPACE = "namespace"
    HPO_OBSOLETE_TERMS = "obsolete_terms"
    HPO_PARENTS = "parents"

class ReactomeNodeField(Enum):
    """
    Enum of all the fields in the reactome dataset. Values are the spellings used
    in the Open Targets parquet files.
    """

    REACTOME_ACCESSION = "id"
    _PRIMARY_ID = REACTOME_ACCESSION

    REACTOME_NAME = 'name'
    REACTOME_LABEL = 'label'
    REACTOME_ANCESTORS = 'ancestors'
    REACTOME_DESCENDANTS = 'descendants'
    REACTOME_CHILDREN = 'children'
    REACTOME_PARENT = 'parents'
    REACTOME_PATH = 'path'