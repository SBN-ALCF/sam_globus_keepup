"""
Utility functions
"""

import os
import pathlib
import subprocess
from typing import Optional


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


def check_env(var: str, val: Optional[str]=None) -> None:
    """Raise if environment variable var is not equal to val."""
    eq_str = ''
    if val is not None:
        eq_str = f'={val}'

    if var in os.environ:
        if val is None:
            # any value OK
            return os.environ[var]

        if os.environ[var] == str(val):
            return os.environ[var]
    
    raise RuntimeError(f"Must set {var}{eq_str} environment variable.")
