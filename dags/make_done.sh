#!/bin/bash


DONE_PATH_FILE=$1

if [ -d "${DONE_PATH_FILE}" ]; then
    figlet "DONE"
    exit 0
else
    echo "I'll be back => ${DONE_PATH_FILE}"
	exit 1
fi
