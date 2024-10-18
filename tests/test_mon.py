import json
import sam_globus_keepup.mon


def test_ifstat():
    """Check if ifstat returns."""
    assert 'lo' in sam_globus_keepup.mon.ifstat()
