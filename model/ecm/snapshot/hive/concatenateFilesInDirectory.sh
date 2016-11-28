#set -x
#!/bin/sh
#
# Combines the contents of all files in a single directory together.
#
# Expects two parameters:
#	1: Directory name containing the source files.
#	2: Target filename including path to create the composite file.
#

if [ $# == 0 ] ; then
    echo "Usage: $0 source_directory destination_file_path"
    exit 1;
fi

logFile="$0.log"
sourceDirectory=$1
targetFilePath=$2

cd ${sourceDirectory}
for file in `ls` 
do
	cat "$file" >> ${targetFilePath}
done
exit