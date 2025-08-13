#!/bin/bash

# manually copy files from dCache to this node
# intended for small numbers of files, e.g., make up a few files that failed to copy
if ! type -P samweb > /dev/null; then
	echo "samweb not found!"
	exit 1
fi

FILELIST=$1
if [ -z ${FILELIST} ]; then
	echo "must provide a file list"
	exit 1
fi

while read line; do
	pnfs_path=$(samweb get-file-access-url $line --schema=file)
	pnfs_path=${pnfs_path#file://}
	rsync -azP sbndpro@sbndgpvm04:${pnfs_path} /srv/globus/sbnd/sbnd/larcv
done < ${FILELIST}
