#!/bin/bash
cd "$(dirname $0)/../"
rm -rf data/tmp data/chan data/spooldir/.flumespool logs
pushd data/spooldir &>/dev/null
for i in *.COMPLETED ; do
	mv "${i}" "${i/.COMPLETED/}"
done
popd &>/dev/null
