import pandas as pd
from collections import Counter

filename = 'data/test_data/diseases'
df = pd.read_parquet(filename)

# Extract only alphabetic characters from the 'id' column and remove numbers and special characters
idd = df['id'].str.replace(r"[^a-zA-Z]", "", regex=True)
print(idd)

print(Counter(list(idd)))
# print(df.columns)
# print(filename + ': ' + str(df.shape))
# print(df.datatypeId.unique())


# build-ot-db   | INFO -- Writing 43959 entries to AssociationByDataSourceDirect-part000.csv
# build-ot-db   | INFO -- Writing 1352 entries to AssociationByDataTypeIndrect-part000.csv
# build-ot-db   | INFO -- Writing 97015 entries to AssociationByDataSourceIndirect-part000.csv
# build-ot-db   | INFO -- Writing 3925 entries to AssociationByDataTypeIndirect-part000.csv
# build-ot-db   | INFO -- Writing 37838 entries to AssociationByOverallDirect-part000.csv
# build-ot-db   | INFO -- Writing 83643 entries to AssociationByOverallIndirect-part000.csv
# build-ot-db   | INFO -- Writing 96104 entries to AssociationByDataTypeIndrect-part001.csv

# data/test_data/associationByDatasourceDirect: (45311, 6)
# data/test_data/associationByDatasourceIndirect: (100940, 6)
# data/test_data/associationByDatatypeDirect: (44991, 5)
# data/test_data/associationByDatatypeIndirect: (100029, 5)
# data/test_data/associationByOverallDirect: (37838, 4)
# data/test_data/associationByOverallIndirect: (83643, 4)
# Counter({'MONDO': 113, 'EFO': 107, 'Orphanet': 26, 'HP': 18, 'GO': 4, 'DOID': 1, 'OBA': 1})