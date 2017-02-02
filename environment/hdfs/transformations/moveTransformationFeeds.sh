#!/bin/bash

if [ $# -ne 1 ] ; then
       echo " Usage: $0 DATA_DIRECTORY"
       exit 1;
fi

dataDirectory=$1
analysisCodeSplitDir=$dataDirectory/analysiscodesplit
policyTransactionDir=$dataDirectory/policytransaction
transactionTypeDir=$dataDirectory/transactiontype

analysisCodeSplitTrustFundDir=$dataDirectory/analysiscodesplittrustfund
analysisCodeSplitRiskCodeDir=$dataDirectory/analysiscodesplitriskcode
policyTransactionWrittenPremiumDir=$dataDirectory/policytransactionwrittenpremium
policyTransactionWrittenDeductionsDir=$dataDirectory/policytransactionwrittendeductions
transactionTypeWrittenDeductionDir=$dataDirectory/transactiontypewrittendeduction
transactionTypeWrittenPremiumDir=$dataDirectory/transactiontypewrittenpremium

hdfs dfs -mkdir $analysisCodeSplitDir $policyTransactionDir $transactionTypeDir

hdfs dfs -mv $analysisCodeSplitTrustFundDir/*.avro $analysisCodeSplitDir
hdfs dfs -mv $analysisCodeSplitRiskCodeDir/*.avro $analysisCodeSplitDir

hdfs dfs -mv $policyTransactionWrittenPremiumDir/*.avro $policyTransactionDir
hdfs dfs -mv $policyTransactionWrittenDeductionsDir/*.avro $policyTransactionDir

hdfs dfs -mv $transactionTypeWrittenDeductionDir/*.avro $transactionTypeDir
hdfs dfs -mv $transactionTypeWrittenPremiumDir/*.avro $transactionTypeDir

hdfs dfs -rm -r -skipTrash $analysisCodeSplitTrustFundDir
hdfs dfs -rm -r -skipTrash $analysisCodeSplitRiskCodeDir
hdfs dfs -rm -r -skipTrash $policyTransactionWrittenPremiumDir
hdfs dfs -rm -r -skipTrash $policyTransactionWrittenDeductionsDir
hdfs dfs -rm -r -skipTrash $transactionTypeWrittenDeductionDir
hdfs dfs -rm -r -skipTrash $transactionTypeWrittenPremiumDir