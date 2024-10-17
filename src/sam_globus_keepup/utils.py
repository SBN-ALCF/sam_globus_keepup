"""
Utility functions
"""

import os
import pathlib
import subprocess


BLOCK_SIZE = 1024


def run_path(run_number: int):
    """Convert 8-digit int ABCXYZPQ to path AB/CX/YZ/PQ."""
    s = f'{run_number:08d}'
    return pathlib.Path(*[s[i:i+2] for i in range(0, len(s), 2)])


def du(path: pathlib.Path) -> int:
    """Return the output of du -s in BLOCK_SIZE bytes."""
    if not path.exists():
        raise RuntimeError(f'Path {path} does not exist.')

    return int(subprocess.check_output(['du','-sx', f'--block-size={BLOCK_SIZE}', path]).split()[0])


def df(path: pathlib.Path) -> int:
    """Return free space of path in BLOCK_SIZE bytes."""
    if not path.exists():
        raise RuntimeError(f'Path {path} does not exist.')

    result = os.statvfs(path)
    return result.f_frsize * result.f_bfree // BLOCK_SIZE
