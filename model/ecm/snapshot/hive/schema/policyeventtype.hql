CREATE TABLE snap_phinsys.policyeventtype
(
	PolicyEventTypeCode varchar(50) comment "The code associated with the Policy Event Type such as 'NEW' for new policy or 'REN' for renewal policy",
	SourceSystemCode varchar(50) comment "The source system associated with the Policy Event Type record",
	PolicyEventTypeDescription varchar(255) comment "The description associated with the Policy Event Type such as 'New' for new policy or 'Renewal' for renewal policy",
	meta_sourcesystemtransactiondatetime timestamp comment "Metadata to identify the date and time that this record was updated on the source system. This should formatted as a JDBC-compliant java.sql.Timestamp:'YYYY-MM-DD HH:MM:SS.fffffffff'.",
	meta_cdcchangesequence varchar(35) comment "Metadata to record the sequence of the changes occurring on the source system.",
	meta_cdcloaddatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Source Data layer.",
	meta_cdcchangeoperation varchar(1) comment "Metadata to record the operation identified by CDC on the source system - one of Update, Insert or Delete.",
	meta_ecmtransformidentifier varchar(35) comment "Metadata to identify the load event which appended this record into the Enstar Conformed Model.",
	meta_ecmtransformdatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Enstar Conformed Model."
);
