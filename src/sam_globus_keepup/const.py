"""
Useful constants
"""

import re

SBND_RAWDATA_REGEXP = re.compile(r".*data_.*run(\d+)_.*\.root")
ICARUS_RAWDATA_REGEXP = re.compile(r".*compressed_data_.*_(.*)_run(\d+)_.*\.root")
