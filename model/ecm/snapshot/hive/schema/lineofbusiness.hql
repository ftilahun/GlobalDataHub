CREATE TABLE snap_phinsys.lineofbusiness
(
	LineOfBusinessCode varchar(50) comment "Code associated with how the business is classified in the source policy administration system for the policy <Dictionary ID='108'/>",
	SubLineOfBusinessCode varchar(50) comment "The code associated with the Company's Sub Line of Business applicable to the risk <Dictionary ID='110'/>",
	SourceSystemCode varchar(50) comment "Source system code that defines which source system the line of business came from",
	LineOfBusinessDescription varchar(255) comment "Description associated with how the business is classified in the source policy administration system for the policy <Dictionary ID='109'/>",
	SubLineOfBusinessDescription varchar(255) comment "The description associated with the Company's Sub Line of Business applicable to the risk <Dictionary ID='111'/>",
	meta_sourcesystemtransactiondatetime timestamp comment "Metadata to identify the date and time that this record was updated on the source system. This should formatted as a JDBC-compliant java.sql.Timestamp:'YYYY-MM-DD HH:MM:SS.fffffffff'.",
	meta_cdcchangesequence varchar(35) comment "Metadata to record the sequence of the changes occurring on the source system.",
	meta_cdcloaddatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Source Data layer.",
	meta_cdcchangeoperation varchar(1) comment "Metadata to record the operation identified by CDC on the source system - one of Update, Insert or Delete.",
	meta_ecmtransformidentifier varchar(35) comment "Metadata to identify the load event which appended this record into the Enstar Conformed Model.",
	meta_ecmtransformdatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Enstar Conformed Model."
);
