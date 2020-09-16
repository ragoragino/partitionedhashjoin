#!/bin/bash

set -e
set -x

Help()
{
   echo "Generates plots from partitioning hash join benchmarks."
   echo
   echo "Syntax: generate.sh [-p]"
   echo "options:"
   echo "-p, --path: Path to the partitioning hash join program binary."
   echo "-s, --skew: Skew parameter."
   echo "-n, --name: Name of the PNG."
   echo
}

concat_columns()
{
	ORIGINAL_FILE=$1
	NEW_CONTENTS_FILE=$2
	COLUMN_TITLE=$3

	{ echo "${COLUMN_TITLE}" & cat ${NEW_CONTENTS_FILE} | jq -r '.results | to_entries[] | "\(.value)"' | tr -d '\r'; } > tmp_jq.txt
	paste -d ' ' ${ORIGINAL_FILE} tmp_jq.txt > tmp_paste.txt && mv tmp_paste.txt ${ORIGINAL_FILE}
	rm tmp_jq.txt
}

while [[ $# -gt 0 ]]; do
case "$1" in
    -p|--path)
    PHJOIN_BINARY_PATH=$(readlink -f "$2")
    shift 2
    ;;
	-s|--skew)
	SKEW=$2
    shift 2
    ;;
	-n|--name)
	NAME=$2
    shift 2
    ;;
	-h|--help)
    Help
    exit 0
    ;;
esac
done

REQUIRED_PARAMS=(${PHJOIN_BINARY_PATH} ${SKEW} ${NAME})

for REQUIRED_PARAM in "${REQUIRED_PARAMS[@]}"; do
    if [[ -z ${REQUIRED_PARAM} ]]; then
		Help
		exit 1
	fi
done

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")

mkdir -p ${SKEW}
cd ${SKEW}

rm -f *

touch figure.dat
printf "NumberOfPartitions\nPartition\nBuild\nProbe" > figure.dat

${PHJOIN_BINARY_PATH} --skew ${SKEW} --join no-partitioning -o file --filename skew_${SKEW}.partitions_1.txt
concat_columns figure.dat skew_${SKEW}.partitions_1.txt NoPartitioning

for PARTITIONS in 32 64 128 256 512 1024 2048 4096 8192; do
	${PHJOIN_BINARY_PATH} --skew 1.05 --join radix-partitioning -p ${PARTITIONS} -o file --filename skew_${SKEW}.partitions_${PARTITIONS}.txt
	concat_columns figure.dat skew_${SKEW}.partitions_${PARTITIONS}.txt Radix${PARTITIONS}
done

gnuplot -e "env_data='figure.dat'" -e "env_figure_title='Comparison of the performance of selected hash join algorithmn for skew: ${SKEW}'"  ${SCRIPT_DIR}/figure.plot > ${NAME}