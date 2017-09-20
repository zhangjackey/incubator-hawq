#! /bin/bash

if [ -z "${HDB_ARTIFACTS}"]; then
	HDB_ARTIFACTS=~/workspace/artifacts/HDB
fi

ls ${HDB_ARTIFACTS}/hdb*
