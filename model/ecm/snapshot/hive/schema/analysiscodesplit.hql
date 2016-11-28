CREATE TABLE IF NOT EXISTS phinsys.analysiscodesplit
(
	PolicyNumber varchar(100) comment "Part of FK to Policy",
	SectionReference varchar(100) comment "Part of FK to Policy",
	CoverageReference varchar(100) comment "Part of FK to Policy",
	SourceSystemCode varchar(50) comment "Part of FK to Policy",
	AnalysisCodeType varchar(50) comment "The type of thing to analyse by, such as 'Risk Code' or 'Trust Fund'",
	AnalysisCode varchar(50) comment "The code to analyse by, for Risk Code this could 'PD' for example",
	AnalaysisCodeDescription varchar(255) comment "Descriptive text for the type of analysis code",
	SourceSystemDescription varchar(255) comment "Description of the source system",
	SplitPercent decimal(12,7) comment "The percentage to split the premium by, for example 95%",
	meta_sourcesystemtransactiondatetime timestamp comment "Metadata to identify the date and time that this record was updated on the source system. This should formatted as a JDBC-compliant java.sql.Timestamp:'YYYY-MM-DD HH:MM:SS.fffffffff'.",
	meta_cdcchangesequence varchar(35) comment "Metadata to record the sequence of the changes occurring on the source system.",
	meta_cdcloaddatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Source Data layer.",
	meta_cdcchangeoperation varchar(1) comment "Metadata to record the operation identified by CDC on the source system - one of Update, Insert or Delete.",
	meta_ecmtransformidentifier varchar(35) comment "Metadata to identify the load event which appended this record into the Enstar Conformed Model.",
	meta_ecmtransformdatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Enstar Conformed Model."
);
