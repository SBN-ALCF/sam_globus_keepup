import pathlib

import sam_globus_keepup 

def test_run_path_3digit():
    assert sam_globus_keepup.utils.run_path(503) == pathlib.Path('00/00/05/03')

def test_run_path_5digit():
    assert sam_globus_keepup.utils.run_path(16503) == pathlib.Path('00/01/65/03')
