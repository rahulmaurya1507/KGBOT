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
        test_size=200
    )

    # Start BioCypher
    bc = BioCypher(
        biocypher_config_path="config/biocypher_config.yaml",
    )

    # Check the schema
    bc.show_ontology_structure()

    # # Write nodes
    # bc.write_nodes(target_disease_adapter.get_nodes())

    # bc.write_edges(target_disease_adapter.get_abo_edges())
    # bc.write_edges(target_disease_adapter.get_abodid_edges())

    # bc.write_edges(target_disease_adapter.get_abds_edges())
    # bc.write_edges(target_disease_adapter.get_abdsdid_edges())
    
    # bc.write_edges(target_disease_adapter.get_abdt_edges())
    # bc.write_edges(target_disease_adapter.get_abdtdid_edges())

    # bc.write_edges(target_disease_adapter.get_dmoa_edges())

    bc.write_edges(target_disease_adapter.get_indication_edges())

    # # Post import functions
    bc.write_import_call()
    bc.summary()

if __name__ == "__main__":
    main()
