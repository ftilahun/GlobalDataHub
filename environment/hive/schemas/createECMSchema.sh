#!/bin/bash

if [ $# -ne 5 ] ; then
    echo "Usage: $0 DATA_DIRECTORY SCHEMA_DIRECTORY DB_NAME DB_COMMENT DB_PROPERTIES (e.g. \'creator\'=\'darragh\',\'date\'=\'2012-01-02\')"
    exit 1;
fi

dataLocation=`echo $1 | sed "s/'//g"`
avroSchemaLocation=`echo $2 | sed "s/'//g"`
dbname=`echo $3 | sed "s/'//g"`
dbComment=`echo $4 | sed "s/'//g"`
dbProperties=$5

declare -a tableNames=(	
                        "analysiscodesplit"
                		"branch"
                		"broker"
                		"currency"
                		"deduction"
                		"deductiontype"
                		"filcode"
                		"geography"
                		"legalentity"
                		"lineofbusiness"
                		"methodofplacement"
                		"policy"
                		"policyeventtype"
                		"policystatus"
                		"policytransaction"
                		"riskcode"
                		"transactiontype"
                		"trustfund"
                		"underwriter"
                	   )

hiveCliCommand="hive -e"

$hiveCliCommand "
CREATE DATABASE IF NOT EXISTS $dbname 
COMMENT '$dbComment'
WITH DBPROPERTIES ($dbProperties);"

for table in "${tableNames[@]}"
do
   $hiveCliCommand "
	CREATE EXTERNAL TABLE IF NOT EXISTS $dbname.$table
	STORED AS AVRO
	LOCATION '$dataLocation/$table'
	tblproperties('avro.schema.url'='$avroSchemaLocation/$table.avsc');"
done