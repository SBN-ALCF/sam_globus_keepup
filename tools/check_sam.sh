#!/bin/bash

# Get list of files in DEFINITION that were not yet consumed

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

samweb list-files "snapshot_id ${SNAP_ID} minus ((project_name like globus_dtn_% and consumed_status 'consumed'))"
