#!/usr/bin/env python

"""
Copies new files from dCache to this node using ifdh, then sends them to
Polaris at ALCF via GLOBUS
"""

import sys
import os
import pathlib

from sam_globus_keepup import EXPERIMENT
from sam_globus_keepup.sam import SAMProjectManager
from sam_globus_keepup.const import SBND_RAWDATA_REGEXP
from sam_globus_keepup.utils import run_path

import logging
logger = logging.getLogger(__name__)


DATASET = "sbnd_keepup_from_17200_raw_Oct14"


def main(project_base: str, output_path: pathlib.Path):
    # sam transfer
    with SAMProjectManager(project_base=project_base, dataset=DATASET) as client:
        for f in client.get_files():
            logger.info(f"Starting transfer of {f}")
            result = SBND_RAWDATA_REGEXP.match(f)
            evb, run_number = (int(g) for g in result.groups())
            outdir = output_path / run_path(run_number)
            client.save_file(dest=outdir)


if __name__ == "__main__":
    if "IFDH_PROXY_ENABLE" not in os.environ:
        raise RuntimeError("Must set IFDH_PROXY_ENABLE=0 environment variable.")

    output_path = pathlib.Path('/scratch/sbnd/rawdata')

    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    main(project_base='globus_dtn_xfer_test', output_path=output_path)
