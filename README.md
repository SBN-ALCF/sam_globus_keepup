# sam_globus_keepup

Utilities to find files via SAM and transfer them via GLOBUS.

## Installation

With `ifdhc` and `sam-web-client` distributed through CVMFS, install in a virtual environment via

```
pip install -e --no-deps .
```

### CVMFS Set up of required packages

With `/cvmfs` mounted, do

```
source /cvmfs/fermilab.opensciencegrid.org/packages/common/setup-env.sh
spack load sam-web-client
spack load ifdhc
```

Versions change frequently and the above commands may return multiple options.
In general, select the latest for your operating system.

## References

- [SAM Documentation](https://cdcvs.fnal.gov/redmine/projects/sam/wiki)
- [IFDHC Documentation](https://cdcvs.fnal.gov/redmine/projects/ifdhc/wiki)
- [GLOBUS SDK](https://globus-sdk-python.readthedocs.io/en/stable/index.html)
