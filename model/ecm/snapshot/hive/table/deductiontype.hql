CREATE TABLE IF NOT EXISTS phinsys.deductiontype
(
	DeductionTypeCode varchar(50) comment "The unique code for the classification of the type of deduction. Deductions can be grouped into various buckets to represent whether it is an acquisition cost or an underwriting credit.",
	SourceSystemCode varchar(50) comment "Source System Identifier for the Deduction Type record",
	DeductionTypeDescription varchar(100) comment "Full Description of the type of Deduction",
	Group varchar(50) comment "Allows for basic grouping or to classify the deductions - such as 'Acquisition Costs' or 'Underwriting Costs'",
	meta_sourcesystemtransactiondatetime timestamp comment "Metadata to identify the date and time that this record was updated on the source system. This should formatted as a JDBC-compliant java.sql.Timestamp:'YYYY-MM-DD HH:MM:SS.fffffffff'.",
	meta_cdcchangesequence varchar(35) comment "Metadata to record the sequence of the changes occurring on the source system.",
	meta_cdcloaddatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Source Data layer.",
	meta_cdcchangeoperation varchar(1) comment "Metadata to record the operation identified by CDC on the source system - one of Update, Insert or Delete.",
	meta_ecmtransformidentifier varchar(35) comment "Metadata to identify the load event which appended this record into the Enstar Conformed Model.",
	meta_ecmtransformdatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Enstar Conformed Model."
);