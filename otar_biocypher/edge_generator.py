from tqdm import tqdm
import hashlib

from .id_utils import _process_id_and_type, _find_licence


class EdgeGenerator:
    def __init__(self, abod_df, aboid_df, abdsd_df, abdsid_df, test_mode=False, test_size=10):
        self.abod_df = abod_df
        self.aboid_df = aboid_df
        self.abdsd_df = abdsd_df
        self.abdsid_df = abdsid_df
        self.test_mode = test_mode
        self.test_size = test_size

    def encoding(self, row):
        return hashlib.md5(str(row).encode()).hexdigest()

    def get_abod_edges(self):
        for _, row in tqdm(self.abod_df.iterrows()):
            edge_id = self.encoding(row)

            disease_id, _ = _process_id_and_type(row['diseaseId'])
            gene_id, _ = _process_id_and_type(row['targetId'], "ensembl")
            properties = {
                'score': row['score'],
                'evidenceCount': row['evidenceCount'],
                'source': row['evidenceCount'],
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
        for _, row in tqdm(self.aboid_df.iterrows()):
            edge_id = self.encoding(row)

            disease_id, _ = _process_id_and_type(row['diseaseId'])
            gene_id, _ = _process_id_and_type(row['targetId'], "ensembl")
            properties = {
                'score': row['score'],
                'evidenceCount': row['evidenceCount'],
                'source': row['evidenceCount'],
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
                'source': _find_licence(row['datasourceId']),
                'licence': 'licence'
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
                'source': _find_licence(row['datasourceId']),
                'licence': 'licence'
            }

            yield (
                edge_id,
                gene_id,
                disease_id,
                row['datatypeId'] + ".abdsid",
                properties
            )
