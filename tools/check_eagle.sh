#!/bin/bash

# Get list of files in DEFINITION that are not present on eagle

if ! type -P samweb > /dev/null; then
	echo "samweb not found!"
	exit 1
fi

DEFINITION=$1

if [ -z ${DEFINITION} ]; then
	echo "Must provide a definition name"
	exit 1
fi

if ! samweb describe-definition ${DEFINITION} > /dev/null 2>&1; then
	echo "Definition ${DEFINITION} not found!"
	exit 1
fi

echo "Checking definition ${DEFINITION}"
SNAP_ID=$(samweb take-snapshot ${DEFINITION})
NFILES_DEF=$(samweb count-files "snapshot_id ${SNAP_ID}")
echo "Snapshot ID is: ${SNAP_ID} (${NFILES_DEF} files)"

SAM_LIST=$(mktemp)
EAGLE_LIST=$(mktemp)
echo "Writing file lists to SAM=${SAM_LIST} and EAGLE=${EAGLE_LIST}"
EAGLE_PATH="/lus/eagle/projects/neutrinoGPU/sbnd/data/larcv/${DEFINITION}"
samweb list-files "snapshot_id ${SNAP_ID}" | sort > ${SAM_LIST}
ssh ${USER}@polaris.alcf.anl.gov "find ${EAGLE_PATH} -type f -exec basename {} \;" | sort > ${EAGLE_LIST}

diff ${SAM_LIST} ${EAGLE_LIST}
