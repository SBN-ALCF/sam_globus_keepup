#!/usr/bin/env python

"""
Copies new files from dCache to this node using ifdh, then sends them to
Polaris at ALCF via GLOBUS
"""

import sys
import os
import pathlib
import logging
from typing import Optional

from sam_globus_keepup import EXPERIMENT
from sam_globus_keepup.sam import SAMProjectManager
from sam_globus_keepup.globus import GLOBUSSessionManager

from sam_globus_keepup.const import SBND_RAWDATA_REGEXP
from sam_globus_keepup.utils import run_path, check_env


def main():
    check_env("IFDH_PROXY_ENABLE")


if __name__ == '__main__':
    main()
