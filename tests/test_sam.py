from sam_globus_keepup import SAMWeb_Client
from sam_globus_keepup.sam import SAM_dataset_exists


DATASET_GOOD = 'sbnd_keepup_from_17200_raw_Oct14'
DATASET_BAD = 'BAD_sbnd_keepup_from_17200_raw_Oct14'

def test_dataset_exists():
    assert SAM_dataset_exists(DATASET_GOOD)

def test_dataset_doesnt_exist():
    assert not SAM_dataset_exists(DATASET_BAD)
