#!/bin/bash
#
#
#
# Stops any running Oozie co-orinators and deploys a new version

OOZIEURL=""
PROPERTIESPATH=""
OOZIEOPTIONS=""


show_help() {
	echo -e "\nUsage:\n$0 -p [path to oozie properties file] -u [oozie url: e.g. http://<OozieHost>:11443/oozie] -o [additional options to pass to oozie e.g '-Dfrequency=*/15 * * * *']"
}

if [  $# -le 3 ]
then
	show_help
	exit 1
fi

while getopts ":u:p:o:" opt; do
    case "$opt" in
    	p)
	PROPERTIESPATH=$OPTARG
        ;;
		u)
	OOZIEURL=$OPTARG
        ;;
		o)
	OOZIEOPTIONS=$OPTARG
        ;;
    \?)
	echo "Invalid option: -$OPTARG" >&2
	exit 1
	;;
    esac
done


export OOZIE_URL=$OOZIEURL

OOZIEIDS=$(oozie jobs -jobtype coordinator -filter status=RUNNING | grep -i "CDC" | grep -E -o "^[[:digit:]].{35}")

# For each job id, kill the job
while IFS=' ' read -ra IDS; do
  for i in "${IDS[@]}"; do
	  oozie job -kill $i
  done
done <<< "$OOZIEIDS"

oozie job -config ${PROPERTIESPATH} -DstartTime=`date -u "+%Y-%m-%dT%H:%MZ"` "${OOZIEOPTIONS}" -submit

