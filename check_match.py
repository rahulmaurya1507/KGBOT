import pandas as pd


abod = pd.read_parquet('data/ot_files/associationByOverallDirect/')
print(abod.columns)
aboid = pd.read_parquet('data/ot_files/associationByOverallIndirect/')
print(aboid.columns)

print(abod.shape, aboid.shape)
print(len(abod.targetId.unique()), len(aboid.targetId.unique()))
print(len(abod.diseaseId.unique()), len(aboid.diseaseId.unique()))

comparision_abod = pd.concat([abod, aboid, aboid]).drop_duplicates(keep=False)
print(comparision_abod.shape)
print(comparision_abod.head())

comparision_aboid = pd.concat([aboid, abod, abod]).drop_duplicates(keep=False)
print(comparision_aboid.shape)
print(comparision_aboid.head())
