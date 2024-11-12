from tqdm import tqdm
import hashlib

from .id_utils import _process_id_and_type, _find_licence


class EdgeGenerator:
    def __init__(self, abo_df, abodid_df, abds_df, abdsdid_df, abdt_df, abdtdid_df, dmoa_df, indications_df):
        self.abo_df = abo_df
        self.abodid_df = abodid_df
        self.abds_df = abds_df
        self.abdsdid_df = abdsdid_df
        self.abdt_df = abdt_df
        self.abdtdid_df = abdtdid_df
        self.dmoa_df = dmoa_df
        self.indications_df = indications_df

    def encoding(self, row):
        return hashlib.md5(str(row).encode()).hexdigest()

    def get_abo_edges(self):
        for _, row in tqdm(self.abo_df.iterrows()):
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
        for _, row in tqdm(self.abodid_df.iterrows()):
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
        for _, row in tqdm(self.abds_df.iterrows()):
            edge_id = self.encoding(row)

            disease_id, _ = _process_id_and_type(row['diseaseId'])
            gene_id, _ = _process_id_and_type(row['targetId'], "ensembl")
            properties = {
                'score': row['score'],
                'evidenceCount': row['evidenceCount'],
                'source': row['datasourceId'],
                'licence': _find_licence(row['datasourceId'])
            }

            yield (
                edge_id,
                gene_id,
                disease_id,
                row['datatypeId'] + ".abds",
                properties
            )

    def get_abdsdid_edges(self):
        for _, row in tqdm(self.abdsdid_df.iterrows()):
            edge_id = self.encoding(row)

            disease_id, _ = _process_id_and_type(row['diseaseId'])
            gene_id, _ = _process_id_and_type(row['targetId'], "ensembl")
            properties = {
                'score': row['score'],
                'evidenceCount': row['evidenceCount'],
                'source': row['datasourceId'],
                'licence': _find_licence(row['datasourceId'])
            }

            yield (
                edge_id,
                gene_id,
                disease_id,
                row['datatypeId'] + ".abdsdid",
                properties
            )

 
    def get_abdt_edges(self):
        for _, row in tqdm(self.abdt_df.iterrows()):
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
                row['datatypeId'] + ".abdt",
                properties
            )

    def get_abdtdid_edges(self):
        for _, row in tqdm(self.abdtdid_df.iterrows()):
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
                row['datatypeId'] + ".abdtdid",
                properties
            )
    
    def get_dmoa_edges(self):
        for _, row in tqdm(self.dmoa_df.iterrows()):
            edge_id = self.encoding(row)
            drug_id, _ = _process_id_and_type(row['chemblIds'], 'chembl')
            gene_id, _ = _process_id_and_type(row['targets'], "ensembl")
            properties = {
                'actionType': row['actionType'],
                'mechanismOfAction': row['mechanismOfAction'],
                'targetName': row['targetName'],
                'targetType': row['targetType'],
                'references': row['references'],
                'source': 'source',
                'licence': 'licence'
            }

            yield (
                edge_id,
                gene_id,
                drug_id,
                'dmoa',
                properties
            )
    
    def get_indication_edges(self):
        for _, row in tqdm(self.indications_df.iterrows()):
            edge_id = self.encoding(row)
            drug_id, _ = _process_id_and_type(row['id'], 'chembl')
            disease_id, _ = _process_id_and_type(row['disease'])
            properties = {
                'indicationCount': row['indicationCount'],
                'approvedIndications': row['approvedIndications'],
                'efoName': row['efoName'],
                'maxPhaseForIndication': row['maxPhaseForIndication'],
                'references': row['references'],
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