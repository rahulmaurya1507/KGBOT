import re

from tqdm import tqdm

from .enums import *

from .id_utils import _process_id_and_type

from biocypher._logger import logger


class NodeGenerator:
    def __init__(self, target_df, disease_df, drug_df, hpo_df, reactome_df, node_fields):
        self.target_df = target_df
        self.disease_df = disease_df
        self.drug_df = drug_df
        self.hpo_df = hpo_df
        self.reactome_df = reactome_df
        self.node_fields = node_fields


    def _yield_node_type(self, df, node_field_type, ontology_class=None):
        """
        Yield the node type from the dataframe.

        Args:
            df: Spark DataFrame containing the node data.
            node_field_type: Enum containing the node fields.
            ontology_class: Ontological class of the node (corresponding to the
                            `label_in_input` field in the schema configuration).
        """
        # Select columns of interest
        df = df.select(
            [
                field.value
                for field in self.node_fields
                if isinstance(field, node_field_type)
            ]  # type: ignore
        )

        logger.info(f"Generating nodes of {node_field_type}.")

        for row in tqdm(df.collect()):
            # normalize id
            if isinstance(row[node_field_type._PRIMARY_ID.value], list):
                input_id = row[node_field_type._PRIMARY_ID.value][0]  # Assuming the first element as primary ID
            else:
                input_id = row[node_field_type._PRIMARY_ID.value]
            _id, _type = _process_id_and_type(input_id, ontology_class)

            if node_field_type == HPONodeField:
                label = row['id']
                _type = re.match(r'^[A-Za-z]+', label).group().lower() + '.hpo'
            if node_field_type == DiseaseNodeField:
                label = row['id']
                _type = re.match(r'^[A-Za-z]+', label).group().lower() + '.disease'
            # if node_field_type == TargetNodeField and row[]:
                

            if not _id:
                continue

            _props = {
                "version": "22.11",
                "source": "Open Targets",
                "licence": "https://platform-docs.opentargets.org/licence"
            }

            for field in self.node_fields:
                if not isinstance(field, node_field_type):
                    continue

                if row[field.value]:
                    _props[field.value] = row[field.value]

            yield (_id, _type, _props)

    def get_nodes(self):
        """
        Yield nodes from the target and disease dataframes.
        """
        # Targets
        yield from self._yield_node_type(
            self.target_df, TargetNodeField, "ensembl"
        )

        # Diseases
        yield from self._yield_node_type(
            self.disease_df, DiseaseNodeField
        )

        yield from self._yield_node_type(
            self.drug_df, DrugNodeField, 'chembl'
        )

        yield from self._yield_node_type(
            self.hpo_df, HPONodeField
        )

        yield from self._yield_node_type(
            self.reactome_df, ReactomeNodeField, 'reactome'
        )
  