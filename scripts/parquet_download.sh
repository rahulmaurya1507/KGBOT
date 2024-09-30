#!/bin/bash

lftp -c 'open ftp://ftp.ebi.ac.uk; mirror --parallel=5 pub/databases/opentargets/platform/24.06/output/etl/parquet/targets ./targets' &
lftp -c 'open ftp://ftp.ebi.ac.uk; mirror --parallel=5 pub/databases/opentargets/platform/24.06/output/etl/parquet/diseases ./diseases' &
lftp -c 'open ftp://ftp.ebi.ac.uk; mirror --parallel=5 pub/databases/opentargets/platform/24.06/output/etl/parquet/molecule ./molecule' &
lftp -c 'open ftp://ftp.ebi.ac.uk; mirror --parallel=5 pub/databases/opentargets/platform/24.06/output/etl/parquet/evidence ./evidence' &
lftp -c 'open ftp://ftp.ebi.ac.uk; mirror --parallel=5 pub/databases/opentargets/platform/24.06/output/etl/parquet/associationByOverallDirect ./associationByOverallDirect' &
lftp -c 'open ftp://ftp.ebi.ac.uk; mirror --parallel=5 pub/databases/opentargets/platform/24.06/output/etl/parquet/associationByOverallIndirect ./associationByOverallIndirect' &
lftp -c 'open ftp://ftp.ebi.ac.uk; mirror --parallel=5 pub/databases/opentargets/platform/24.06/output/etl/parquet/associationByDatasourceDirect ./associationByDatasourceDirect' &
lftp -c 'open ftp://ftp.ebi.ac.uk; mirror --parallel=5 pub/databases/opentargets/platform/24.06/output/etl/parquet/associationByDatasourceIndirect ./associationByDatasourceIndirect' &
lftp -c 'open ftp://ftp.ebi.ac.uk; mirror --parallel=5 pub/databases/opentargets/platform/24.06/output/etl/parquet/associationByDatatypeDirect ./associationByDatatypeDirect' &
lftp -c 'open ftp://ftp.ebi.ac.uk; mirror --parallel=5 pub/databases/opentargets/platform/24.06/output/etl/parquet/associationByDatatypeIndirect ./associationByDatatypeIndirect' &

lftp -c 'open ftp://ftp.ebi.ac.uk; mirror --parallel=5 pub/databases/opentargets/platform/24.06/output/etl/parquet/interaction ./interaction' &
lftp -c 'open ftp://ftp.ebi.ac.uk; mirror --parallel=5 pub/databases/opentargets/platform/24.06/output/etl/parquet/interactionEvidence ./interactionEvidence' &
lftp -c 'open ftp://ftp.ebi.ac.uk; mirror --parallel=5 pub/databases/opentargets/platform/24.06/output/etl/parquet/baselineExpression ./baselineExpression' &
lftp -c 'open ftp://ftp.ebi.ac.uk; mirror --parallel=5 pub/databases/opentargets/platform/24.06/output/etl/parquet/go ./go' &
lftp -c 'open ftp://ftp.ebi.ac.uk; mirror --parallel=5 pub/databases/opentargets/platform/24.06/output/etl/parquet/mousePhenotypes ./mousePhenotypes' &
lftp -c 'open ftp://ftp.ebi.ac.uk; mirror --parallel=5 pub/databases/opentargets/platform/24.06/output/etl/parquet/diseaseToPhenotype ./diseaseToPhenotype' &
lftp -c 'open ftp://ftp.ebi.ac.uk; mirror --parallel=5 pub/databases/opentargets/platform/24.06/output/etl/parquet/hpo ./hpo' &
lftp -c 'open ftp://ftp.ebi.ac.uk; mirror --parallel=5 pub/databases/opentargets/platform/24.06/output/etl/parquet/mechanismOfAction ./mechanismOfAction' &
lftp -c 'open ftp://ftp.ebi.ac.uk; mirror --parallel=5 pub/databases/opentargets/platform/24.06/output/etl/parquet/indication ./indication' &
lftp -c 'open ftp://ftp.ebi.ac.uk; mirror --parallel=5 pub/databases/opentargets/platform/24.06/output/etl/parquet/fda/significantAdverseDrugReactions ./significantAdverseDrugReactions' &

lftp -c 'open ftp://ftp.ebi.ac.uk; mirror --parallel=5 pub/databases/opentargets/platform/24.06/output/etl/parquet/drugWarnings ./drugWarnings' &
lftp -c 'open ftp://ftp.ebi.ac.uk; mirror --parallel=5 pub/databases/opentargets/platform/24.06/output/etl/parquet/pharmacogenomics ./pharmacogenomics' &
lftp -c 'open ftp://ftp.ebi.ac.uk; mirror --parallel=5 pub/databases/opentargets/platform/24.06/output/etl/parquet/reactome ./reactome' &
lftp -c 'open ftp://ftp.ebi.ac.uk; mirror --parallel=5 pub/databases/opentargets/platform/24.06/output/etl/parquet/targetPrioritisation ./targetPrioritisation' &
lftp -c 'open ftp://ftp.ebi.ac.uk; mirror --parallel=5 pub/databases/opentargets/platform/24.06/output/etl/parquet/targetEssentiality ./targetEssentiality' &

wait


# nohup scripts/parquet_download.sh > download.log 2>&1 &