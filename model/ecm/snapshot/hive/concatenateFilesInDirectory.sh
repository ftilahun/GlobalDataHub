#set -x
#!/bin/sh
#
# Combines the contents of all files from a single directory into a single file.
# The use case for this utility is combining all SQL files into a single SQL script to run with the Beeline CLI.
#
# Expects two parameters:
#	1: Directory name containing the source files.
#	2: Target filename including path to create the composite file.
#
if [ ${#} -ne 2 ] || ! [ -d ${1} ]; then
    echo "Usage: $0 source_directory destination_file_name"
    exit 1;
fi

logFile="$0.log"
sourceDirectory=$1
targetFileName=$2
scriptDirectory=`pwd`

cd ${sourceDirectory}
for file in `ls -p | grep -v /` 
do
	echo ${file}
	cat ${file} >> ${scriptDirectory}/${targetFileName}
done
cd ${scriptDirectory}

exit
