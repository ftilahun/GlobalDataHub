CREATE TABLE IF NOT EXISTS phinsys.insured
(
	InsuredCode varchar(50) comment "The code used to identify an organisation/person/persons who is the subject of the direct insurance policy",
	SourceSystemCode varchar(50) comment "The source system code associated with the insured",
	InsuredLegalName varchar(255) comment "The name under which the insured is incorporated or chartered",
	InsuredName varchar(255) comment "The name used to identify an organisation/person/persons who is the subject of the direct insurance policy",
	meta_sourcesystemtransactiondatetime timestamp comment "Metadata to identify the date and time that this record was updated on the source system. This should formatted as a JDBC-compliant java.sql.Timestamp:'YYYY-MM-DD HH:MM:SS.fffffffff'.",
	meta_cdcchangesequence varchar(35) comment "Metadata to record the sequence of the changes occurring on the source system.",
	meta_cdcloaddatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Source Data layer.",
	meta_cdcchangeoperation varchar(1) comment "Metadata to record the operation identified by CDC on the source system - one of Update, Insert or Delete.",
	meta_ecmtransformidentifier varchar(35) comment "Metadata to identify the load event which appended this record into the Enstar Conformed Model.",
	meta_ecmtransformdatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Enstar Conformed Model."
);
