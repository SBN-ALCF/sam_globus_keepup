"""
Useful constants
"""

import re

SBND_RAWDATA_REGEXP = re.compile(r".*data_evb(\d+)_.*run(\d+)_.*\.root")
ICARUS_RAWDATA_REGEXP = re.compile(r".*compressed_data_.*_(.*)_run(\d+)_.*\.root")
