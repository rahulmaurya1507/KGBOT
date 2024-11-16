from bioregistry import normalize_curie

# The identifier you have
curie = "R-HSA:141444"

# Normalize the CURIE
normalized = normalize_curie(f"reactome:{curie}")

print(f"Original CURIE: {curie}")
print(f"Normalized CURIE: {normalized}")
