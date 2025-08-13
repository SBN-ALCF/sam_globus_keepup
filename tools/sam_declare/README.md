# SAM Declare

Declare files quickly with Python multiprocessing! Accepts single files or a
directory. Adds checksums & file size fields to metadata

## Run

Set up the `samweb_client` Spack package:

```
source "/cvmfs/fermilab.opensciencegrid.org/packages/common/setup-env.sh"
spack load /d366jj6 # sam-web-client 
```

Then do

```
python sam_declare.py -h
```

for program options and usage.
