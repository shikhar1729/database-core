#!/usr/bin/env bash

function verify() {
	arr=("$@")
	for i in "${arr[@]}";
		do
				if [ ! -f $i ]; then

					echo "Missing ${i}"
					exit 1
				fi
		done
}

req_files=("src/include/common/macros.h")
verify "${req_files[@]}"	
if [[ $? -ne 0 ]]; then
    exit 1
fi

if [ $# -eq 1 ]
then
	zip "${1}.zip" src/include/operators/operators.h src/operators/operators.cc REPORT.md
else
	echo 'Please provide a file name, eg ./submit Gaurav'
fi
