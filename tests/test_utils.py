import os
import pathlib

import pytest

import sam_globus_keepup.utils


ENV_VAR = '____TEST'


def test_run_path_3digit():
    assert sam_globus_keepup.utils.run_path(503) == pathlib.Path('00/00/05/03')

def test_run_path_5digit():
    assert sam_globus_keepup.utils.run_path(16503) == pathlib.Path('00/01/65/03')

def test_check_env_any():
    """Any value OK if none specified."""
    os.environ[ENV_VAR] = 'dummy' 
    sam_globus_keepup.utils.check_env(ENV_VAR)
    
def test_check_env_eq():
    """Check for equality."""
    os.environ[ENV_VAR] = '1' 
    sam_globus_keepup.utils.check_env(ENV_VAR, '1')

def test_check_env_ret():
    """Check for return value."""
    os.environ[ENV_VAR] = '1' 
    assert sam_globus_keepup.utils.check_env(ENV_VAR) == '1'

def test_check_env_noeq():
    """Check for runtime error raise on not equal."""
    os.environ[ENV_VAR] = 'dummy' 
    with pytest.raises(RuntimeError) as e:
        sam_globus_keepup.utils.check_env(ENV_VAR, '1')

    assert str(e.value) == f'Must set {ENV_VAR}=1 environment variable.'

def test_check_env_unset():
    """Check for runtime error raise on not set."""
    try:
        del os.environ[ENV_VAR]
    except KeyError:
        pass

    with pytest.raises(RuntimeError) as e:
        sam_globus_keepup.utils.check_env(ENV_VAR)

    assert str(e.value) == f'Must set {ENV_VAR} environment variable.'
