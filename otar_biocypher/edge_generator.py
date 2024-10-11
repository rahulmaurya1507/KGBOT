from tqdm import tqdm
import hashlib

from .id_utils import _process_id_and_type, _find_licence


class EdgeGenerator:
    def __init__(self, abod_df, aboid_df, abdsd_df, abdsid_df, abdtd_df, abdtid_df, test_mode=False, test_size=10):
        self.abod_df = abod_df
        self.aboid_df = aboid_df
        self.abdsd_df = abdsd_df
        self.abdsid_df = abdsid_df
        self.abdtd_df = abdtd_df
        self.abdtid_df = abdtid_df
        self.test_mode = test_mode
        self.test_size = test_size
    def encoding(self, row):
        return hashlib.md5(str(row).encode()).hexdigest()

    def get_abod_edges(self):
        if self.test_mode:
            self.abod_df = self.abod_df.head(self.test_size)
        for _, row in tqdm(self.abod_df.iterrows()):
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
                'abod',
                properties
            )

    def get_aboid_edges(self):
        if self.test_mode:
            self.aboid_df = self.aboid_df.head(self.test_size)
        for _, row in tqdm(self.aboid_df.iterrows()):
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
                'aboid',
                properties
            )

    def get_abdsd_edges(self):
        if self.test_mode:
            self.abdsd_df = self.abdsd_df.head(self.test_size)
        for _, row in tqdm(self.abdsd_df.iterrows()):
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
                row['datatypeId'] + ".abdsd",
                properties
            )

    def get_abdsid_edges(self):
        if self.test_mode:
            self.abdsid_df = self.abdsid_df.head(self.test_size)
        for _, row in tqdm(self.abdsid_df.iterrows()):
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
                row['datatypeId'] + ".abdsid",
                properties
            )

 
    def get_abdtd_edges(self):
        if self.test_mode:
            self.abdtd_df = self.abdtd_df.head(self.test_size)
        for _, row in tqdm(self.abdtd_df.iterrows()):
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
                row['datatypeId'] + ".abdtd",
                properties
            )

    def get_abdtid_edges(self):
        if self.test_mode:
            self.abdtid_df = self.abdtid_df.head(self.test_size)
        for _, row in tqdm(self.abdtid_df.iterrows()):
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
                row['datatypeId'] + ".abdtid",
                properties
            )