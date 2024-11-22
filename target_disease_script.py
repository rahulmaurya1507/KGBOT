import sys

from biocypher import BioCypher

from otar_biocypher.fields import (
    target_disease_datasets,
    node_fields
)

from otar_biocypher.ota import (
    TargetDiseaseEvidenceAdapter
)

# VSCode does not add the root directory to the path (by default?). Not sure why
# this works sometimes and not others. This is a workaround.


def main():
    """
    Main function running the import using BioCypher and the adapter.
    """
    # Open Targets
    target_disease_adapter = TargetDiseaseEvidenceAdapter(
        datasets=target_disease_datasets,
        node_fields=node_fields,
        test_size=2000
    )

    # Start BioCypher
    bc = BioCypher(
        biocypher_config_path="config/biocypher_config.yaml",
    )

    # Check the schema
    bc.show_ontology_structure()

    # Write nodes
    bc.write_nodes(target_disease_adapter.get_nodes())

    # Updated code for processing edges in batches

    # ABO edges
    target_disease_adapter.edge_generator.abo_df = target_disease_adapter.get_edge_batches(target_disease_adapter.edge_generator.abo_df)
    for batch in target_disease_adapter.edge_generator.current_batches:
        bc.write_edges(target_disease_adapter.edge_generator.get_abo_edges(batch_number=batch))

    # ABODID edges
    target_disease_adapter.edge_generator.abodid_df = target_disease_adapter.get_edge_batches(target_disease_adapter.edge_generator.abodid_df)
    for batch in target_disease_adapter.edge_generator.current_batches:
        bc.write_edges(target_disease_adapter.edge_generator.get_abodid_edges(batch_number=batch))

    # ABDS edges
    target_disease_adapter.edge_generator.abds_df = target_disease_adapter.get_edge_batches(target_disease_adapter.edge_generator.abds_df)
    for batch in target_disease_adapter.edge_generator.current_batches:
        bc.write_edges(target_disease_adapter.edge_generator.get_abds_edges(batch_number=batch))

    # ABDSDID edges
    target_disease_adapter.edge_generator.abdsdid_df = target_disease_adapter.get_edge_batches(target_disease_adapter.edge_generator.abdsdid_df)
    for batch in target_disease_adapter.edge_generator.current_batches:
        bc.write_edges(target_disease_adapter.edge_generator.get_abdsdid_edges(batch_number=batch))

    # ABDT edges
    target_disease_adapter.edge_generator.abdt_df = target_disease_adapter.get_edge_batches(target_disease_adapter.edge_generator.abdt_df)
    for batch in target_disease_adapter.edge_generator.current_batches:
        bc.write_edges(target_disease_adapter.edge_generator.get_abdt_edges(batch_number=batch))

    # ABDTDID edges
    target_disease_adapter.edge_generator.abdtdid_df = target_disease_adapter.get_edge_batches(target_disease_adapter.edge_generator.abdtdid_df)
    for batch in target_disease_adapter.edge_generator.current_batches:
        bc.write_edges(target_disease_adapter.edge_generator.get_abdtdid_edges(batch_number=batch))

    # Molecular Interactions edges
    target_disease_adapter.edge_generator.molecular_interactions_df = target_disease_adapter.get_edge_batches(target_disease_adapter.edge_generator.molecular_interactions_df)
    for batch in target_disease_adapter.edge_generator.current_batches:
        bc.write_edges(target_disease_adapter.edge_generator.get_molecular_interactions_edges(batch_number=batch))

    # DMOA edges
    target_disease_adapter.edge_generator.dmoa_df = target_disease_adapter.get_edge_batches(target_disease_adapter.edge_generator.dmoa_df)
    for batch in target_disease_adapter.edge_generator.current_batches:
        bc.write_edges(target_disease_adapter.edge_generator.get_dmoa_edges(batch_number=batch))

    # Indication edges
    target_disease_adapter.edge_generator.indications_df = target_disease_adapter.get_edge_batches(target_disease_adapter.edge_generator.indications_df)
    for batch in target_disease_adapter.edge_generator.current_batches:
        bc.write_edges(target_disease_adapter.edge_generator.get_indication_edges(batch_number=batch))

    # Disease-to-Phenotype edges
    target_disease_adapter.edge_generator.disease2phenotype_df = target_disease_adapter.get_edge_batches(target_disease_adapter.edge_generator.disease2phenotype_df)
    for batch in target_disease_adapter.edge_generator.current_batches:
        bc.write_edges(target_disease_adapter.edge_generator.get_disease2phenotype_edges(batch_number=batch))

    # Interaction Evidence edges
    target_disease_adapter.edge_generator.interaction_evidence_df = target_disease_adapter.get_edge_batches(target_disease_adapter.edge_generator.interaction_evidence_df)
    for batch in target_disease_adapter.edge_generator.current_batches:
        bc.write_edges(target_disease_adapter.edge_generator.get_interaction_evidence_edges(batch_number=batch))


    # Post import functions
    bc.write_import_call()
    bc.summary()

if __name__ == "__main__":
    main()


# node time without batching:  63.37005138397217
# abo time with batching: time:  96.28948855400085