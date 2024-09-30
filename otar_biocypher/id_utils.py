import functools
from typing import Optional
from bioregistry.resolve import normalize_curie

from .enums import _licences


@functools.lru_cache()
def _process_id_and_type(inputId: str, _type: Optional[str] = None):
    """
    Process diseaseId and diseaseType fields from evidence data. Process
    gene (ENSG) ids.

    Args:

        inputId: id of the node.

        _type: type of the node.
    """

    if not inputId:
        return (None, None)

    _id = None

    if _type:
        _id = normalize_curie(f"{_type}:{inputId}")

        return (_id, _type)

    # detect delimiter (either _ or :)
    if "_" in inputId:
        _type = inputId.split("_")[0].lower()

        # special case for OTAR TODO
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


def _find_licence(source: str) -> str:
    """
    Find and return the licence for a source.

    Args:

        source: source of the evidence. Spelling as in the Open Targets
        evidence data.
    """

    return _licences.get(source, "Unknown")
