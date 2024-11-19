import time
import hashlib
from tqdm import tqdm

from .id_utils import _process_id_and_type, _find_licence


class EdgeGenerator:
    def __init__(
            self, 
            abo_df,
            abodid_df, 
            abds_df, 
            abdsdid_df,
            abdt_df,
            abdtdid_df,
            dmoa_df,
            indications_df,
            molecular_interactions_df,
            disease2phenotype_df,
            interaction_evidence_df
    ):
        self.abo_df = abo_df
        self.abodid_df = abodid_df
        self.abds_df = abds_df
        self.abdsdid_df = abdsdid_df
        self.abdt_df = abdt_df
        self.abdtdid_df = abdtdid_df
        self.dmoa_df = dmoa_df
        self.indications_df = indications_df
        self.molecular_interactions_df = molecular_interactions_df
        self.disease2phenotype_df = disease2phenotype_df
        self.interaction_evidence_df = interaction_evidence_df

    def encoding(self, row):
        return hashlib.md5(str(row).encode()).hexdigest()

    def get_abo_edges(self):
        self.abo_df.collect()
        for row in tqdm(self.abo_df.collect()):
            edge_id = self.encoding(row)

            disease_id, _ = _process_id_and_type(row['diseaseId'])
            gene_id, _ = _process_id_and_type(row['targetId'], "ensembl")
            properties = {
                'score': row['score'],
                'evidenceCount': row['evidenceCount'],
                'source': 'source',
                'licence': 'licence'
            }

            yield (
                edge_id,
                gene_id,
                disease_id,
                'abo',
                properties
            )
    
    def get_abodid_edges(self):
        for row in tqdm(self.abodid_df.collect()):
            edge_id = self.encoding(row)

            disease_id, _ = _process_id_and_type(row['diseaseId'])
            gene_id, _ = _process_id_and_type(row['targetId'], "ensembl")
            properties = {
                'score': row['score'],
                'evidenceCount': row['evidenceCount'],
                'source': 'source',
                'licence': 'licence'
            }

            yield (
                edge_id,
                gene_id,
                disease_id,
                'abodid',
                properties
            )

    def get_abds_edges(self):
        for row in tqdm(self.abds_df.collect()):
            edge_id = self.encoding(row)
            disease_id, _ = _process_id_and_type(row['diseaseId'])
            gene_id, _ = _process_id_and_type(row['targetId'], "ensembl")
            yield (
                edge_id,
                gene_id,
                disease_id,
                f"{row['datatypeId']}.abds",
                {
                    'score': row['score'],
                    'evidenceCount': row['evidenceCount'],
                    'source': row['datasourceId'],
                    'licence': _find_licence(row['datasourceId'])
                }
            )

    def get_abdsdid_edges(self):
        for row in self.abdsdid_df.collect():
            edge_id = self.encoding(row)
            disease_id, _ = _process_id_and_type(row['diseaseId'])
            gene_id, _ = _process_id_and_type(row['targetId'], "ensembl")

            yield (
                edge_id,
                gene_id,
                disease_id,
                f"{row['datatypeId']}.abdsdid",
                {
                'score': row['score'],
                'evidenceCount': row['evidenceCount'],
                'source': row['datasourceId'],
                'licence': _find_licence(row['datasourceId'])
            }
            )

 
    def get_abdt_edges(self):
        for row in tqdm(self.abdt_df.collect()):
            edge_id = self.encoding(row)
            disease_id, _ = _process_id_and_type(row['diseaseId'])
            gene_id, _ = _process_id_and_type(row['targetId'], "ensembl")
            yield (
                edge_id,
                gene_id,
                disease_id,
                f"{row['datatypeId']}.abdt", {
                'score': row['score'],
                'evidenceCount': row['evidenceCount'],
                'source': 'source',
                'licence': 'licence'
            }
            )

    def get_abdtdid_edges(self):
        for row in tqdm(self.abdtdid_df.collect()):
            edge_id = self.encoding(row)
            disease_id, _ = _process_id_and_type(row['diseaseId'])
            gene_id, _ = _process_id_and_type(row['targetId'], "ensembl")
            yield (
                edge_id,
                gene_id,
                disease_id,
                f"{row['datatypeId']}.abdtdid",
                {
                    'score': row['score'],
                    'evidenceCount': row['evidenceCount'],
                    'source': 'source',
                    'licence': 'licence'
                }
            )
    
    def get_dmoa_edges(self):
        for row in tqdm(self.dmoa_df.collect()):
            edge_id = self.encoding(row)
            drug_id, _ = _process_id_and_type(row['chemblIds'], 'chembl')
            gene_id, _ = _process_id_and_type(row['targets'], "ensembl")
            properties = {
                key: row[key] for key in [
                    'actionType', 
                    'mechanismOfAction', 
                    'targetName', 
                    'targetType', 
                    'references'
                ]
            }
            properties.update({'source': 'source', 'licence': 'licence'})

            yield edge_id, gene_id, drug_id, 'dmoa', properties
    
    def get_indication_edges(self):
        for row in tqdm(self.indications_df.collect()):
            edge_id = self.encoding(row)
            drug_id, _ = _process_id_and_type(row['id'], 'chembl')
            disease_id, _ = _process_id_and_type(row['disease'])
            # print(row['references'])
            references = [row.asDict() for row in row['references']]
            references = list(map(str, references))
            properties = {
                'indicationCount': row['indicationCount'],
                'approvedIndications': row['approvedIndications'],
                'efoName': row['efoName'],
                'maxPhaseForIndication': row['maxPhaseForIndication'],
                'references': references,
                'source': 'source',
                'licence': 'licence'
            }

            yield (
                edge_id,
                drug_id,
                disease_id,
                'indications',
                properties
            )

    def get_molecular_interactions_edges(self):
        for row in tqdm(self.molecular_interactions_df.collect()):
            edge_id = self.encoding(row)
            src_gene_id, _ = _process_id_and_type(row['targetA'], "ensembl")
            tar_gene_id, _ = _process_id_and_type(row['targetB'], "ensembl")
            properties = {
                'sourceDatabase': row['sourceDatabase'],
                'intA': row['intA'],
                'intABiologicalRole': row['intABiologicalRole'],
                'intB': row['intB'],
                'intBBiologicalRole': row['intBBiologicalRole'],
                'speciesA': row['speciesA'].asDict(),
                'speciesB': row['speciesB'].asDict(),
                'count': row['count'],
                'scoring': row['scoring'],
                'source': 'source',
                'licence': 'licence'
            }

            yield (
                edge_id,
                src_gene_id,
                tar_gene_id,
                'molecular_interactions',
                properties
            )
    
    def get_disease2phenotype_edges(self):
        for row in tqdm(self.disease2phenotype_df.collect()):
            edge_id = self.encoding(row)
            disease_id, _ = _process_id_and_type(row['disease'])
            phenotype_id, _ = _process_id_and_type(row['phenotype'])

            evidence = [row.asDict() for row in row['evidence']]
            evidence = list(map(str, evidence))

            properties = {
                'evidence': evidence,
                'source': 'source',
                'licence': 'licence'
            }

            yield (
                edge_id,
                disease_id,
                phenotype_id,
                'disease_phenotype',
                properties
            )

    def get_interaction_evidence_edges(self):
        for row in tqdm(self.interaction_evidence_df.collect()):
            edge_id = self.encoding(row)
            src_id, _ = _process_id_and_type(row['targetA'], "ensembl")
            tar_id, _ = _process_id_and_type(row['targetB'], "ensembl")
            
            # participantDetectionMethodA = [item.asDict() for item in row['participantDetectionMethodA']] if row['participantDetectionMethodA'] else []
            # participantDetectionMethodA = list(map(str, participantDetectionMethodA))

            # participantDetectionMethodB = [item.asDict() for item in row['participantDetectionMethodB']]  if row['participantDetectionMethodB'] else []
            # participantDetectionMethodB = list(map(str, participantDetectionMethodB))

            properties = {
                'hostOrganismTissue': row['hostOrganismTissue'].asDict() if row['hostOrganismTissue'] else {},
                'evidenceScore': row['evidenceScore'],
                'intBBiologicalRole': row['intBBiologicalRole'],
                'interactionResources': row['interactionResources'].asDict() if row['interactionResources'] else {},
                'interactionTypeMiIdentifier': row['interactionTypeMiIdentifier'],
                'interactionDetectionMethodShortName': row['interactionDetectionMethodShortName'],
                'intA': row['intA'],
                'intBSource': row['intBSource'],
                'speciesB': row['speciesB'].asDict(),
                'interactionIdentifier': row['interactionIdentifier'],
                'hostOrganismTaxId': row['hostOrganismTaxId'],
                'participantDetectionMethodA': row['participantDetectionMethodA'],
                'expansionMethodShortName': row['expansionMethodShortName'],
                'speciesA': row['speciesA'].asDict() if row['speciesA'] else {},
                'intASource': row['intASource'],
                'intB': row['intB'],
                'pubmedId': row['pubmedId'],
                'intABiologicalRole': row['intABiologicalRole'],
                'hostOrganismScientificName': row['hostOrganismScientificName'],
                'interactionScore': row['interactionScore'],
                'interactionTypeShortName': row['interactionTypeShortName'],
                'expansionMethodMiIdentifier': row['expansionMethodMiIdentifier'],
                'participantDetectionMethodB': row['participantDetectionMethodB'],
                'interactionDetectionMethodMiIdentifier': row['interactionDetectionMethodMiIdentifier'],
                'source': 'source',
                'licence': 'licence'
            }

            yield (
                edge_id,
                src_id,
                tar_id,
                'evidence_molecular_interactions',
                properties
            )