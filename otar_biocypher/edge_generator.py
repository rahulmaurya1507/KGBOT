import hashlib
from tqdm import tqdm

from biocypher._logger import logger

from pyspark.sql import DataFrame, functions as F

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
            interaction_evidence_df,
            targets_go_df
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
        self.targets_go_df = targets_go_df

    def encoding(self, row):
        return hashlib.md5(str(row).encode()).hexdigest()
    
    def get_edge_batches(self, df: DataFrame) -> DataFrame:
        """
        Adds partition number to the evidence dataframe and returns the data
        frame.

        Args:

            df: The evidence dataframe.

        Returns:

            The evidence dataframe with a new column "partition_num" containing
            the partition number.
        """

        logger.info("Generating batches.")

        # add partition number to self.df as column
        df = df.withColumn("partition_num", F.spark_partition_id())
        df.persist()

        self.current_batches = [
            int(row.partition_num)
            for row in df.select("partition_num").distinct().collect()
        ]
        logger.info(f"Generated {len(self.current_batches)} batches.")

        return df

    def get_abo_edges(self, batch_number):
        # Check if df has column partition_num
        if "partition_num" not in self.abo_df.columns:
            raise ValueError(
                "df does not have column partition_num. "
                "Please run get_edge_batches() first."
            )

        logger.info("Generating Gene -> Disease Overall edges.")

        logger.info(
            f"Processing batch {batch_number+1} of {len(self.current_batches)}."
        )

        yield from self._process_abo_edges(
            self.abo_df.where(self.abo_df.partition_num == batch_number)
        )

    def _process_abo_edges(self, batch: DataFrame):
        rows = batch.collect()

        for row in tqdm(rows):
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
    
    def get_abodid_edges(self, batch_number):
        # Check if df has column partition_num
        if "partition_num" not in self.abodid_df.columns:
            raise ValueError(
                "df does not have column partition_num. "
                "Please run get_edge_batches() first."
            )

        logger.info("Generating Gene -> Disease Overall direct indirect edges.")

        logger.info(
            f"Processing batch {batch_number+1} of {len(self.current_batches)}."
        )

        yield from self._process_abodid_edges(
            self.abodid_df.where(self.abodid_df.partition_num == batch_number)
        )
    
    def _process_abodid_edges(self, batch: DataFrame):
        """
        Process one batch of Gene -> Disease Overall edges.

        Args:
            batch: Spark DataFrame containing the edges of one batch.
        """
        rows = batch.collect()
        for row in tqdm(rows):
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


    def get_abds_edges(self, batch_number):
        # Check if df has column partition_num
        if "partition_num" not in self.abds_df.columns:
            raise ValueError(
                "df does not have column partition_num. "
                "Please run get_edge_batches() first."
            )

        logger.info("Generating Gene -> Disease datasource edges.")

        logger.info(
            f"Processing batch {batch_number+1} of {len(self.current_batches)}."
        )

        yield from self._process_abds_edges(
            self.abds_df.where(self.abds_df.partition_num == batch_number)
        )


    def _process_abds_edges(self, batch: DataFrame):
        rows = batch.collect()
        for row in tqdm(rows):
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

    def get_abdsdid_edges(self, batch_number):
        # Check if df has column partition_num
        if "partition_num" not in self.abdsdid_df.columns:
            raise ValueError(
                "df does not have column partition_num. "
                "Please run get_edge_batches() first."
            )

        logger.info("Generating Gene -> Disease datasource direct indirect edges.")

        logger.info(
            f"Processing batch {batch_number+1} of {len(self.current_batches)}."
        )

        yield from self._process_abdsid_edges(
            self.abdsdid_df.where(self.abdsdid_df.partition_num == batch_number)
        )

    def _process_abdsid_edges(self, batch: DataFrame):
        rows = batch.collect()
        for row in tqdm(rows):
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

 
    def get_abdt_edges(self, batch_number):
        # Check if df has column partition_num
        if "partition_num" not in self.abdt_df.columns:
            raise ValueError(
                "df does not have column partition_num. "
                "Please run get_edge_batches() first."
            )

        logger.info("Generating Gene -> Disease datatype edges.")

        logger.info(
            f"Processing batch {batch_number+1} of {len(self.current_batches)}."
        )

        yield from self._process_abdt_edges(
            self.abdt_df.where(self.abdt_df.partition_num == batch_number)
        )

    def _process_abdt_edges(self, batch: DataFrame):
        rows = batch.collect()
        for row in tqdm(rows):
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


    def get_abdtdid_edges(self, batch_number):
        # Check if df has column partition_num
        if "partition_num" not in self.abdtdid_df.columns:
            raise ValueError(
                "df does not have column partition_num. "
                "Please run get_edge_batches() first."
            )

        logger.info("Generating Gene -> Disease datatype direct indirect edges.")

        logger.info(
            f"Processing batch {batch_number+1} of {len(self.current_batches)}."
        )

        yield from self._process_abdtdid_edges(
            self.abdtdid_df.where(self.abdtdid_df.partition_num == batch_number)
        )

    def _process_abdtdid_edges(self, batch: DataFrame):
        rows = batch.collect()
        for row in tqdm(rows):
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
    
    def get_dmoa_edges(self, batch_number):
        # Check if df has column partition_num
        if "partition_num" not in self.dmoa_df.columns:
            raise ValueError(
                "df does not have column partition_num. "
                "Please run get_edge_batches() first."
            )

        logger.info("Generating drug mechanism of action edges.")

        logger.info(
            f"Processing batch {batch_number+1} of {len(self.current_batches)}."
        )

        yield from self._process_dmoa_edges(
            self.dmoa_df.where(self.dmoa_df.partition_num == batch_number)
        )

    def _process_dmoa_edges(self, batch: DataFrame):
        rows = batch.collect()
        for row in tqdm(rows):
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
    

    def get_indication_edges(self, batch_number):
        # Check if df has column partition_num
        if "partition_num" not in self.indications_df.columns:
            raise ValueError(
                "df does not have column partition_num. "
                "Please run get_edge_batches() first."
            )

        logger.info("Generating drug indication edges.")

        logger.info(
            f"Processing batch {batch_number+1} of {len(self.current_batches)}."
        )

        yield from self._process_indication_edges(
            self.indications_df.where(self.indications_df.partition_num == batch_number)
        )

    def _process_indication_edges(self, batch: DataFrame):
        rows = batch.collect()
        for row in tqdm(rows):
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

    def get_molecular_interactions_edges(self, batch_number):
        # Check if df has column partition_num
        if "partition_num" not in self.molecular_interactions_df.columns:
            raise ValueError(
                "df does not have column partition_num. "
                "Please run get_edge_batches() first."
            )

        logger.info("Generating molecular indications edges.")

        logger.info(
            f"Processing batch {batch_number+1} of {len(self.current_batches)}."
        )

        yield from self._process_moleculer_interations_edges(
            self.molecular_interactions_df.where(self.molecular_interactions_df.partition_num == batch_number)
        )
            
    def _process_moleculer_interations_edges(self, batch: DataFrame):
        rows = batch.collect()
        for row in tqdm(rows):
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
    
    def get_disease2phenotype_edges(self, batch_number):
        # Check if df has column partition_num
        if "partition_num" not in self.disease2phenotype_df.columns:
            raise ValueError(
                "df does not have column partition_num. "
                "Please run get_edge_batches() first."
            )

        logger.info("Generating disease to phenotype edges.")

        logger.info(
            f"Processing batch {batch_number+1} of {len(self.current_batches)}."
        )

        yield from self._process_disease2phenotype_edges(
            self.disease2phenotype_df.where(self.disease2phenotype_df.partition_num == batch_number)
        )

    def _process_disease2phenotype_edges(self, batch: DataFrame):
        rows = batch.collect()
        for row in tqdm(rows):
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

    def get_interaction_evidence_edges(self, batch_number):
        # Check if df has column partition_num
        if "partition_num" not in self.interaction_evidence_df.columns:
            raise ValueError(
                "df does not have column partition_num. "
                "Please run get_edge_batches() first."
            )

        logger.info("Generating interaction evidence edges.")

        logger.info(
            f"Processing batch {batch_number+1} of {len(self.current_batches)}."
        )

        yield from self._process_interation_evidence_edges(
            self.interaction_evidence_df.where(self.interaction_evidence_df.partition_num == batch_number)
        )

    def _process_interation_evidence_edges(self, batch: DataFrame):
        rows = batch.collect()
        for row in tqdm(rows):
            edge_id = self.encoding(row)
            src_id, _ = _process_id_and_type(row['targetA'], "ensembl")
            tar_id, _ = _process_id_and_type(row['targetB'], "ensembl")
            
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

    def get_targets_go_edges(self, batch_number):
            # Check if df has column partition_num
            if "partition_num" not in self.targets_go_df.columns:
                raise ValueError(
                    "df does not have column partition_num. "
                    "Please run get_edge_batches() first."
                )

            logger.info("Generating target to go edges.")

            logger.info(
                f"Processing batch {batch_number+1} of {len(self.current_batches)}."
            )

            yield from self._process_targets_go_edges(
                self.targets_go_df.where(self.targets_go_df.partition_num == batch_number)
            )

    def _process_targets_go_edges(self, batch: DataFrame):
        rows = batch.collect()
        for row in tqdm(rows):
            edge_id = self.encoding(row)
            go_id, _ = _process_id_and_type(row['goId'], 'go')
            gene_id, _ = _process_id_and_type(row['ensemblId'], 'ensembl')


            properties = {
                'evidence': row['goEvidence'],
                'source': row['goSource'],
                'licence': 'licence',
                'version': '22.11'
            }

            yield (
                edge_id,
                gene_id,
                go_id,
                'GENE_TO_GO_TERM_ASSOCIATION',
                properties
            )