from sam_globus_keepup.const import SBND_RAWDATA_REGEXP


FILENAME = 'data_evb04_EventBuilder4_art1_run17204_25_20241013T025419.root'


def test_sbnd_regexp():
    assert len(SBND_RAWDATA_REGEXP.match(FILENAME).groups()) == 1

def test_sbnd_regexp_run():
    assert SBND_RAWDATA_REGEXP.match(FILENAME).groups()[0] == '17204'

