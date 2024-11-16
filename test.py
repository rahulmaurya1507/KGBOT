import time
import hashlib
from typing import Optional
from bioregistry.resolve import normalize_curie
from pyspark.sql import SparkSession

# Create SparkSession
spark = (
    SparkSession.builder.master("local")  # type: ignore
    .appName("otar_biocypher")
    .config('spark.driver.memory', '4g')
    .config('spark.executor.memory', '4g')
    .master('local[*]')
    .getOrCreate()
)

# Read data
abo_path = "data/pot_files/abo"
abo_df = spark.read.parquet(abo_path)

def _process_id_and_type(inputId: str, _type: Optional[str] = None):
    if not inputId:
        return (None, None)

    _id = None

    if _type:
        _id = normalize_curie(f"{_type}:{inputId}")
        return (_id, _type)

    if "_" in inputId:
        _type = inputId.split("_")[0].lower()
        if _type == "otar":
            _id = f"otar:{inputId.split('_')[1]}"
        else:
            _id = normalize_curie(inputId, sep="_")
    elif ":" in inputId:
        _type = inputId.split(":")[0].lower()
        _id = normalize_curie(inputId, sep=":")

    if not _id:
        return (None, None)

    return (_id, _type)

# Optimized row processing function
def process_row(row):
    edge_id = hashlib.md5(str(row).encode()).hexdigest()
    disease_id, _ = _process_id_and_type(row['diseaseId'])
    gene_id, _ = _process_id_and_type(row['targetId'], "ensembl")
    properties = {
        'score': row['score'],
        'evidenceCount': row['evidenceCount'],
        'source': 'source',
        'licence': 'licence'
    }
    return (edge_id, gene_id, disease_id, 'abo', properties)

# Apply optimizations: Limit data, repartition, and cache before processing
abo_df = abo_df.limit(200000)  # Use a limited subset of data
print('count data: ', abo_df.count())
# Repartition and cache before performing any transformation
abo_df = abo_df.repartition(100).cache()

# Perform transformations and time the operation
start = time.time()

# Directly use DataFrame API if possible for better optimization
processed_df = abo_df.rdd.map(process_row)  # Apply the transformation
processed_rdd = processed_df.repartition(1000)  # Repartition for parallelism

results = processed_rdd.collect()
end = time.time()
print('process run time: ', end - start)
