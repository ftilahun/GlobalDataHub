CREATE TABLE snap_phinsys.transactiontype
(
	TransactionTypeCode varchar(50) comment "Code associated with the type of transaction <Dictionary ID="123"/>",
	TransactionSubTypeCode varchar(50) comment "The code associated with the type of transaction. This is a subset of transaction type <Dictionary ID="125"/>",
	SourceSystemCode varchar(50),
	TransactionTypeDescription varchar(100) comment "Description associated with the type of transaction <Dictionary ID="124"/>",
	TransactionSubTypeDescription varchar(100) comment "The description associated with the type of transaction. This is a subset of transaction type <Dictionary ID="126"/>",
	Group varchar(50) comment "Allows for basic grouping of transactions, such as "USM" or "SCM"",
	IsCashTransactionType boolean comment "An indicator which identifies whether the transaction is a cash or non-cash transaction <Dictionary ID="3"/>",
	meta_sourcesystemtransactiondatetime timestamp comment "Metadata to identify the date and time that this record was updated on the source system. This should formatted as a JDBC-compliant java.sql.Timestamp:'YYYY-MM-DD HH:MM:SS.fffffffff'.",
	meta_cdcchangesequence varchar(35) comment "Metadata to record the sequence of the changes occurring on the source system.",
	meta_cdcloaddatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Source Data layer.",
	meta_cdcchangeoperation varchar(1) comment "Metadata to record the operation identified by CDC on the source system - one of Update, Insert or Delete.",
	meta_ecmtransformidentifier varchar(35) comment "Metadata to identify the load event which appended this record into the Enstar Conformed Model.",
	meta_ecmtransformdatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Enstar Conformed Model."
)
