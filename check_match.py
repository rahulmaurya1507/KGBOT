import pandas as pd


abod = pd.read_parquet('data/ot_files/associationByOverallIndirect/part-00003-c94918cb-23a8-4467-971d-48174b9df6b2-c000.snappy.parquet')
abod.to_csv('csv/aboid.csv', index=False)
print(abod.head())
targets = pd.read_parquet('data/ot_files/targets/part-00000-6dfa6a61-ee3c-4f00-9923-b874aea463d7-c000.snappy.parquet')

abod_target = list(abod[abod['diseaseId'] == "DOID_7551"]['targetId'])

target_id = list(targets['id'])

for at in abod_target:
    if at in target_id:
        print(at)