CREATE TABLE snap_phinsys.branch
(
	BranchCode varchar(50) comment "The code associated with the company branch through which the business has been placed <Dictionary ID="128"/>",
	SourceSystemCode varchar(50) comment "The source system code of the branch",
	BranchDescription varchar(255) comment "The description associated with the company branch through which the business has been placed <Dictionary ID="58"/>",
	BranchLocationCode varchar(50) comment "FK to geography",
	meta_sourcesystemtransactiondatetime timestamp comment "Metadata to identify the date and time that this record was updated on the source system. This should formatted as a JDBC-compliant java.sql.Timestamp:'YYYY-MM-DD HH:MM:SS.fffffffff'.",
	meta_cdcchangesequence varchar(35) comment "Metadata to record the sequence of the changes occurring on the source system.",
	meta_cdcloaddatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Source Data layer.",
	meta_cdcchangeoperation varchar(1) comment "Metadata to record the operation identified by CDC on the source system - one of Update, Insert or Delete.",
	meta_ecmtransformidentifier varchar(35) comment "Metadata to identify the load event which appended this record into the Enstar Conformed Model.",
	meta_ecmtransformdatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Enstar Conformed Model."
)
