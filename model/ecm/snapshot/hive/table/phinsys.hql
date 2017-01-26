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
CREATE TABLE IF NOT EXISTS phinsys.branch
(
	BranchCode varchar(50) comment "The code associated with the company branch through which the business has been placed <Dictionary ID='128'/>",
	SourceSystemCode varchar(50) comment "The source system code of the branch",
	BranchDescription varchar(255) comment "The description associated with the company branch through which the business has been placed <Dictionary ID='58'/>",
	BranchLocationCode varchar(50) comment "FK to geography",
	meta_sourcesystemtransactiondatetime timestamp comment "Metadata to identify the date and time that this record was updated on the source system. This should formatted as a JDBC-compliant java.sql.Timestamp:'YYYY-MM-DD HH:MM:SS.fffffffff'.",
	meta_cdcchangesequence varchar(35) comment "Metadata to record the sequence of the changes occurring on the source system.",
	meta_cdcloaddatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Source Data layer.",
	meta_cdcchangeoperation varchar(1) comment "Metadata to record the operation identified by CDC on the source system - one of Update, Insert or Delete.",
	meta_ecmtransformidentifier varchar(35) comment "Metadata to identify the load event which appended this record into the Enstar Conformed Model.",
	meta_ecmtransformdatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Enstar Conformed Model."
);
CREATE TABLE IF NOT EXISTS phinsys.broker
(
	BrokerCode varchar(50) comment "The code associated with the broker",
	SourceSystemCode varchar(50) comment "The source system code associated with the broker record",
	BrokerDescription varchar(255) comment "The description of the broker",
	meta_sourcesystemtransactiondatetime timestamp comment "Metadata to identify the date and time that this record was updated on the source system. This should formatted as a JDBC-compliant java.sql.Timestamp:'YYYY-MM-DD HH:MM:SS.fffffffff'.",
	meta_cdcchangesequence varchar(35) comment "Metadata to record the sequence of the changes occurring on the source system.",
	meta_cdcloaddatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Source Data layer.",
	meta_cdcchangeoperation varchar(1) comment "Metadata to record the operation identified by CDC on the source system - one of Update, Insert or Delete.",
	meta_ecmtransformidentifier varchar(35) comment "Metadata to identify the load event which appended this record into the Enstar Conformed Model.",
	meta_ecmtransformdatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Enstar Conformed Model."
);
CREATE TABLE IF NOT EXISTS phinsys.currency
(
	CurrencyCode varchar(50),
	SourceSystemCode varchar(50),
	CurrencyDescription varchar(100),
	meta_sourcesystemtransactiondatetime timestamp comment "Metadata to identify the date and time that this record was updated on the source system. This should formatted as a JDBC-compliant java.sql.Timestamp:'YYYY-MM-DD HH:MM:SS.fffffffff'.",
	meta_cdcchangesequence varchar(35) comment "Metadata to record the sequence of the changes occurring on the source system.",
	meta_cdcloaddatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Source Data layer.",
	meta_cdcchangeoperation varchar(1) comment "Metadata to record the operation identified by CDC on the source system - one of Update, Insert or Delete.",
	meta_ecmtransformidentifier varchar(35) comment "Metadata to identify the load event which appended this record into the Enstar Conformed Model.",
	meta_ecmtransformdatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Enstar Conformed Model."
);
CREATE TABLE IF NOT EXISTS phinsys.deduction
(
	DeductionReference varchar(100) comment "Unique deduction reference from the source system",
	SourceSystemCode varchar(50) comment "Source System Identifier for the Deduction record",
	CalculatedDeductionAmount decimal(18,6),
	CoverageReference varchar(100) comment "Coverage reference identifies unique coverages on a policy section, a section may contain multiple coverages",
	CurrencyCode varchar(50),
	DeductionSequence int,
	DeductionTypeCode varchar(50) comment "The classification of the type of deduction. Deductions can be grouped into various buckets to represent whether it is an acquisition cost or an underwriting credit.",
	DeductionTypeDescription varchar(100) comment "Full Description of the type of Deduction",
	DeductionTypeGroup varchar(50) comment "Allows for basic grouping or to classify the deductions - such as 'Acquisition Costs' or 'Underwriting Costs'",
	IsNetOfPrevious boolean,
	IsPercentage boolean comment "Is the deduction value, expressed as a financial amount or a percentage?",
	PolicyNumber varchar(100) comment "Unique policy number, also known Policy Reference or Underwriting Reference",
	SectionReference varchar(100) comment "Section reference identifies a unique section on a policy, a policy may contain multiple sections",
	SourceSystemDescription varchar(255) comment "Description of the source system",
	Value decimal(18,6) comment "The deduction value, expressed as a financial amount or a percentage",
	meta_sourcesystemtransactiondatetime timestamp comment "Metadata to identify the date and time that this record was updated on the source system. This should formatted as a JDBC-compliant java.sql.Timestamp:'YYYY-MM-DD HH:MM:SS.fffffffff'.",
	meta_cdcchangesequence varchar(35) comment "Metadata to record the sequence of the changes occurring on the source system.",
	meta_cdcloaddatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Source Data layer.",
	meta_cdcchangeoperation varchar(1) comment "Metadata to record the operation identified by CDC on the source system - one of Update, Insert or Delete.",
	meta_ecmtransformidentifier varchar(35) comment "Metadata to identify the load event which appended this record into the Enstar Conformed Model.",
	meta_ecmtransformdatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Enstar Conformed Model."
);
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
);CREATE TABLE IF NOT EXISTS phinsys.filcode
(
	FILCode varchar(4),
	SourceSystemCode varchar(50),
	FILCodeDescription varchar(255),
	meta_sourcesystemtransactiondatetime timestamp comment "Metadata to identify the date and time that this record was updated on the source system. This should formatted as a JDBC-compliant java.sql.Timestamp:'YYYY-MM-DD HH:MM:SS.fffffffff'.",
	meta_cdcchangesequence varchar(35) comment "Metadata to record the sequence of the changes occurring on the source system.",
	meta_cdcloaddatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Source Data layer.",
	meta_cdcchangeoperation varchar(1) comment "Metadata to record the operation identified by CDC on the source system - one of Update, Insert or Delete.",
	meta_ecmtransformidentifier varchar(35) comment "Metadata to identify the load event which appended this record into the Enstar Conformed Model.",
	meta_ecmtransformdatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Enstar Conformed Model."
);
CREATE TABLE IF NOT EXISTS phinsys.geography
(
	GeographyCode varchar(50) comment "Unique ISO region code or custom MDM code",
	GeographyTypeCode varchar(50) comment "Description of the level of Geography",
	GeographyTypeDescription varchar(100) comment "Full Description of the Geography Type",
	GeographyDescription varchar(100) comment "Full Description of the geography item.",
	ParentGeographyCode varchar(50) comment "FK lookup to Geography table - Pointer to the Parent record for building regional hierarchy",
	ParentSourceSystemCode varchar(50) comment "FK lookup to Geography table - Pointer to the Parent record for building regional hierarchy",
	SourceSystemDescription varchar(255) comment "Description of the source system",
	meta_sourcesystemtransactiondatetime timestamp comment "Metadata to identify the date and time that this record was updated on the source system. This should formatted as a JDBC-compliant java.sql.Timestamp:'YYYY-MM-DD HH:MM:SS.fffffffff'.",
	meta_cdcchangesequence varchar(35) comment "Metadata to record the sequence of the changes occurring on the source system.",
	meta_cdcloaddatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Source Data layer.",
	meta_cdcchangeoperation varchar(1) comment "Metadata to record the operation identified by CDC on the source system - one of Update, Insert or Delete.",
	meta_ecmtransformidentifier varchar(35) comment "Metadata to identify the load event which appended this record into the Enstar Conformed Model.",
	meta_ecmtransformdatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Enstar Conformed Model."
);
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
CREATE TABLE IF NOT EXISTS phinsys.legalentity
(
	LegalEntityCode varchar(50) comment "The code associated with the Legal Entity stored on the policy <Dictionary ID='2'/>",
	SourceSystemCode varchar(50) comment "The source system code associated with the Legal Entity record from the policy admin system",
	LegalEntityDescription varchar(255) comment "The description associated with the Legal Entity stored on the policy <Dictionary ID='131'/>",
	ParentLegalEntityCode varchar(50),
	ParentLegalEntitySourceSystemCode varchar(50),
	SourceSystemDescription varchar(255) comment "Description of the source system",
	meta_sourcesystemtransactiondatetime timestamp comment "Metadata to identify the date and time that this record was updated on the source system. This should formatted as a JDBC-compliant java.sql.Timestamp:'YYYY-MM-DD HH:MM:SS.fffffffff'.",
	meta_cdcchangesequence varchar(35) comment "Metadata to record the sequence of the changes occurring on the source system.",
	meta_cdcloaddatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Source Data layer.",
	meta_cdcchangeoperation varchar(1) comment "Metadata to record the operation identified by CDC on the source system - one of Update, Insert or Delete.",
	meta_ecmtransformidentifier varchar(35) comment "Metadata to identify the load event which appended this record into the Enstar Conformed Model.",
	meta_ecmtransformdatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Enstar Conformed Model."
);
CREATE TABLE IF NOT EXISTS phinsys.limitedexcess
(
	LimitExcessReference varchar(100) comment "Unique reference for a limit excess record",
	SourceSystemCode varchar(50) comment "The source system associated with a limit excess",
	Amount decimal(18,6) comment "The amount of the limit excess",
	CoverageReference varchar(100),
	CurrencyCode varchar(50) comment "The currency of the limit excess",
	LimitExcessTypeCode varchar(50),
	LimitExcessTypeDescription varchar(255) comment "The description of the limit excess type",
	PolicyNumber varchar(100),
	SectionReference varchar(100),
	SourceSystemDescription varchar(255),
	meta_sourcesystemtransactiondatetime timestamp comment "Metadata to identify the date and time that this record was updated on the source system. This should formatted as a JDBC-compliant java.sql.Timestamp:'YYYY-MM-DD HH:MM:SS.fffffffff'.",
	meta_cdcchangesequence varchar(35) comment "Metadata to record the sequence of the changes occurring on the source system.",
	meta_cdcloaddatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Source Data layer.",
	meta_cdcchangeoperation varchar(1) comment "Metadata to record the operation identified by CDC on the source system - one of Update, Insert or Delete.",
	meta_ecmtransformidentifier varchar(35) comment "Metadata to identify the load event which appended this record into the Enstar Conformed Model.",
	meta_ecmtransformdatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Enstar Conformed Model."
);
CREATE TABLE IF NOT EXISTS phinsys.lineofbusiness
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
CREATE TABLE IF NOT EXISTS phinsys.methodofplacement
(
	MethodOfPlacementCode varchar(50) comment "The code associated with the Method of Placement",
	SourceSystemCode varchar(50) comment "The source system code that the method of placement record is loaded from",
	MethodOfPlacementDescription varchar(255) comment "The description associated with the Method of Placement",
	meta_sourcesystemtransactiondatetime timestamp comment "Metadata to identify the date and time that this record was updated on the source system. This should formatted as a JDBC-compliant java.sql.Timestamp:'YYYY-MM-DD HH:MM:SS.fffffffff'.",
	meta_cdcchangesequence varchar(35) comment "Metadata to record the sequence of the changes occurring on the source system.",
	meta_cdcloaddatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Source Data layer.",
	meta_cdcchangeoperation varchar(1) comment "Metadata to record the operation identified by CDC on the source system - one of Update, Insert or Delete.",
	meta_ecmtransformidentifier varchar(35) comment "Metadata to identify the load event which appended this record into the Enstar Conformed Model.",
	meta_ecmtransformdatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Enstar Conformed Model."
);
CREATE TABLE IF NOT EXISTS phinsys.policy
(
	PolicyNumber varchar(50) comment "Unique policy number, also known Policy Reference or Underwriting Reference (Dictionary ID=7)",
	CoverageReference varchar(100) comment "Coverage reference identifies unique coverages on a policy section, a section may contain multiple coverages",
	SectionReference varchar(100) comment "Section reference identifies a unique section on a policy, a policy may contain multiple sections",
	SourceSystemCode varchar(50) comment "FK to Source System",
	BillingDueDate date comment "Due date of policy billings",
	BillingTerms varchar(100) ,
	BranchCode varchar(50) comment "FK to Branch",
	BranchDescription varchar(255) comment "The description associated with the company branch through which the business has been placed",
	BrokerCode varchar(50) ,
	BrokerDescription varchar(255) comment "The description of the broker",
	CancellationDate timestamp comment "The date the policy was canceled (Dictionary ID=10)",
	CommutationIndicator boolean comment "Indicates whether commuted or not 1=Commuted  0=Not Commuted  The commutation status on a policy contract. Commutation is the termination of a reinsurance contract by agreement of the parties, on the basis of one or more lump sum payments by the reinsurer, which extinguish its liability under the contract. The payment made by the reinsurer commonly relates to incurred losses under the contract.",
	CoverageDescription varchar(255) comment "Associated description of the type of coverage of a policy",
	CoverageEffectiveFromDate timestamp comment "This would usually be the same as the policy inception date but may differ, for example when an auto coverage is added to a policy 6 months into the policy.  This date will be needed for earnings.",
	CoverageEffectiveToDate timestamp comment "This would usually be the same as the policy expiry date but may differ, for example when an auto coverage is added mid term but only covers a couple of months  This date will be needed for earnings.",
	DirectBillOrAgencyBill varchar(50) comment "For Direct Bill business, G/L carries balance due from Insured and separate Commission A/P due to producer.  For Agency Bill business, G/L carries single A/R number (Balance due from Insured less Commission A/P to agent)",
	DirectOrReinsurance varchar(50) comment "Direct or Reinsurance indicator (Direct or Reinsurance)",
	EstimatedSignedPercent decimal(12,7) comment "The estimated signed line percentage based on expected signing down of the requested written line percentage. Also known as Estimated Signed Down (ESD) (Dictionary ID=105)",
	ExpiryDate timestamp comment "The expiry date of the risk/policy (Dictionary ID=9)",
	FILCode varchar(50) comment "The full FIL code of the policy, including FIL 2 and FIL 4",
	InceptionDate timestamp comment "The date that cover commences (Dictionary ID=8)",
	InsuredCode varchar(50) ,
	InsuredDomicileCode varchar(50) comment "FK to Geography",
	InsuredLegalName varchar(255) comment "The name under which the insured is incorporated or chartered",
	InsuredName varchar(255) comment "The name used to identify an organisation/person/persons who is the subject of the direct insurance policy",
	LegacyPolicyNumber varchar(100) comment "The original policy reference from the legacy data source (Dictionary ID=122)",
	LegalEntityCode varchar(50) comment "FK to Legal entity",
	LineOfBusinessCode varchar(50) comment "FK to line of business",
	LineOfBusinessDescription varchar(255) comment "Description associated with how the business is classified in the source policy administration system for the policy",
	LinkedMasterReference varchar(100) comment "The master reference to which multiple policies/line slips/declarations/bordereaux may be associated (e.g. a London Market Programme Policy Reference to which a number of Policies/Layers will be associated) (Dictionary ID=98)",
	LiveOrLegacy varchar(6) comment "Determines whether the Policy is Live or Legacy",
	MajorRiskCode varchar(50) comment "The code associated with business placed through Lloyd's of London, where the business is classified as per Lloyd's reporting requirements. This classification is usually recorded in the source policy administration system. There may be more than one per policy, however this is the major risk code which will be one code only.",
	MajorTrustFundCode varchar(50) comment "The major trust fund for the policy. This is the Premium Trust Fund (PTF), which is a regulatory requirement when placing business through Lloyd's of London. The business of the Lloyd's syndicate is conducted through the PTF under a standard Deed approved by Lloyd's. The trustees of the fund are appointed by managing agents and approved by Lloyd's. The PTF is a fund into which all premiums are paid, and from which all claims and expenses are paid. It is held for the benefit of policyholders until such time as a profit or loss is declared.",
	MethodOfPlacementCode varchar(50) comment "FK to method of placement",
	MethodOfPlacementDescription varchar(255) comment "The description associated with the Method of Placement",
	PolicyEventTypeCode varchar(50) comment "FK to policy type code",
	PolicyEventTypeDescription varchar(255) comment "The description associated with the Policy Event Type such as 'New' for new policy or 'Renewal' for renewal policy.",
	PolicyStatusCode varchar(50) comment "FK to policy status code.",
	PolicyStatusDescription varchar(50) comment "The description associated with the Policy Status, such as Cancelled.",
	ProgramIdentifier float comment "e.g. - Wellington, London Aviation, etc.",
	RiskLocationCode varchar(50) comment "FK to Geography",
	ReservingSegment varchar(50) ,
	ShareOfWholePercent decimal(12,7) comment "Our indicative percentage share of whole risk using (in order, depending on availability) signed, estimated or written line percent. E.g. where order = 50% and signed line = 10%, Share of Whole Percentage is 5%. (Also known as Reporting Line %)  This will be calculated using order and line percentages stored on this policy table.",
	SegmentIdentifier varchar(100) comment "Codes that allow for segregation for analysis by functional areas (e.g. - healthcare vs MPL, etc)",
	SignedOrderPercent decimal(12,7) comment "The total signed order on the slip as a percentage of the 100% whole risk. This super-cedes the written order and it is determined by the broker (Dictionary ID=104)",
	SignedLinePercent decimal(12,7) comment "Final actual committed share %. It may be the same as the underwriter’s written line and will reflect the Estimated Signing or, if/when there is signing down, a lower amount (Dictionary ID=101)",
	SourceSystemDescription varchar(255) ,
	SourceSystemPolicyNumber varchar(100) comment "The policy reference number associated with the underwritten policy in the policy administration system  (Dictionary ID=121)",
	SubLineOfBusinessCode varchar(50) comment "FK to LineOfBusiness",
	SubLineOfBusinessDescription varchar(255) comment "FK to LineOfBusiness",
	UnderwriterCode varchar(50) ,
	UnderwriterName varchar(255) comment "The name associated with the underwriter that underwrote the risk",
	UniqueMarketReference varchar(100) comment "Unique Market Reference for the policy e.g. (UMR) for Lloyd's Market.  It may be blank where there is no market wide reference (Dictionary ID=91)",
	WrittenDate timestamp comment "Written Date is the date that the policy was written, not to be confused with the Inception date, which is the date that insurance cover commences.",
	WrittenLinePercent decimal(12,7) comment "The amount of a risk that an underwriter is willing to accept on behalf of the members of the syndicate or company for which he underwrites. This is commonly expressed as a percentage of the sum insured which is written on the broker’s placing slip. Can also be shown as an Amount (with associated currency) (Dictionary ID=102).",
	WrittenOrderPercent decimal(12,7) comment "The percentage written order of the slip (Dictionary ID=103).",
	YearOfAccount int comment "The calendar year in which the policy incepts. For syndicate business, if a policy incepts in January 2014, and a declaration is received in January 2015 for that policy, its underwriting year will still be 2014. Also known as Underwriting Year (Dictionary ID=112).",
	meta_sourcesystemtransactiondatetime timestamp comment "Metadata to identify the date and time that this record was updated on the source system. This should formatted as a JDBC-compliant java.sql.Timestamp:'YYYY-MM-DD HH:MM:SS.fffffffff'.",
	meta_cdcchangesequence varchar(35) comment "Metadata to record the sequence of the changes occurring on the source system.",
	meta_cdcloaddatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Source Data layer.",
	meta_cdcchangeoperation varchar(1) comment "Metadata to record the operation identified by CDC on the source system - one of Update, Insert or Delete.",
	meta_ecmtransformidentifier varchar(35) comment "Metadata to identify the load event which appended this record into the Enstar Conformed Model.",
	meta_ecmtransformdatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Enstar Conformed Model."
	);CREATE TABLE IF NOT EXISTS phinsys.policyeventtype
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
CREATE TABLE IF NOT EXISTS phinsys.policystatus
(
	PolicyStatusCode varchar(50) comment "The code associated with the Policy Status, such as 'CAN' for cancelled",
	SourceSystemCode varchar(50) comment "The source system code associated with the policy status record loaded from source",
	PolicyStatusDescription varchar(50) comment "The description associated with the Policy Status, such as 'Cancelled'",
	meta_sourcesystemtransactiondatetime timestamp comment "Metadata to identify the date and time that this record was updated on the source system. This should formatted as a JDBC-compliant java.sql.Timestamp:'YYYY-MM-DD HH:MM:SS.fffffffff'.",
	meta_cdcchangesequence varchar(35) comment "Metadata to record the sequence of the changes occurring on the source system.",
	meta_cdcloaddatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Source Data layer.",
	meta_cdcchangeoperation varchar(1) comment "Metadata to record the operation identified by CDC on the source system - one of Update, Insert or Delete.",
	meta_ecmtransformidentifier varchar(35) comment "Metadata to identify the load event which appended this record into the Enstar Conformed Model.",
	meta_ecmtransformdatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Enstar Conformed Model."
);
CREATE TABLE IF NOT EXISTS phinsys.policytransaction
(
	TransactionReference varchar(100) comment "Unique transaction reference, part of the PK",
	SourceSystemCode varchar(50) comment "FK to Source system and part of PK",
	AccountingAmount decimal(18,6) comment "Transaction accounting amount",
	AccountingCurrencyCode varchar(50) comment "FK to Currency",
	AccountingCurrencyDescription varchar(100),
	AccountingPeriod int comment "The period the transaction will be recognized in the general ledger",
	AccountingRateOfExchange decimal(18,6) comment "ROE for conversion from original to accounting if present",
	CoverageReference varchar(100) comment "FK to Policy",
	FILCode varchar(4),
	FILCodeDescription varchar(255),
	IsCashTransactionType boolean,
	OriginalAmount decimal(18,6) comment "Transaction original amount <Dictionary ID='16'/>",
	OriginalCurrencyCode varchar(50) comment "FK to Currency <Dictionary ID='13'/>",
	OriginalCurrencyDescription varchar(100) comment "Description of original currency",
	PolicyNumber varchar(100) comment "FK to Policy",
	RiskCode varchar(2),
	RiskCodeDescription varchar(255),
	SectionReference varchar(100) comment "FK to Policy",
	SettlementAmount decimal(18,6) comment "Transaction settlement amount <Dictionary ID='99'/>",
	SettlementCurrencyCode varchar(50) comment "FK to Currency <Dictionary ID='14'/>",
	SettlementCurrencyDescription varchar(100),
	SettlementRateOfExchange decimal(18,6) comment "ROE for conversion from original to settlement if present",
	SourceSystemDescription varchar(255) comment "Description of the source system",
	TransactionDate timestamp comment "The date the transaction was loaded into the source system, or if not available then date loaded into GDH",
	TransactionSubTypeCode varchar(50) comment "FK to Transaction Type", 
	TransactionSubTypeDescription varchar(50),
	TransactionTypeCode varchar(50) comment "FK to Transaction Type",
	TransactionTypeDescription varchar(100) comment "Transaction type description",
	TransactionTypeGroup varchar(50) comment "Allows for basic grouping of transactions, such as 'USM' or 'SCM'",
	TrustFundCode varchar(2),
	TrustFundCodeDescription varchar(255),
	meta_sourcesystemtransactiondatetime timestamp comment "Metadata to identify the date and time that this record was updated on the source system. This should formatted as a JDBC-compliant java.sql.Timestamp:'YYYY-MM-DD HH:MM:SS.fffffffff'.",
	meta_cdcchangesequence varchar(35) comment "Metadata to record the sequence of the changes occurring on the source system.",
	meta_cdcloaddatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Source Data layer.",
	meta_cdcchangeoperation varchar(1) comment "Metadata to record the operation identified by CDC on the source system - one of Update, Insert or Delete.",
	meta_ecmtransformidentifier varchar(35) comment "Metadata to identify the load event which appended this record into the Enstar Conformed Model.",
	meta_ecmtransformdatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Enstar Conformed Model."
);
CREATE TABLE IF NOT EXISTS phinsys.riskcode
(
	RiskCode varchar(2),
	SourceSystemCode varchar(50),
	RiskCodeDescription varchar(255),
	meta_sourcesystemtransactiondatetime timestamp comment "Metadata to identify the date and time that this record was updated on the source system. This should formatted as a JDBC-compliant java.sql.Timestamp:'YYYY-MM-DD HH:MM:SS.fffffffff'.",
	meta_cdcchangesequence varchar(35) comment "Metadata to record the sequence of the changes occurring on the source system.",
	meta_cdcloaddatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Source Data layer.",
	meta_cdcchangeoperation varchar(1) comment "Metadata to record the operation identified by CDC on the source system - one of Update, Insert or Delete.",
	meta_ecmtransformidentifier varchar(35) comment "Metadata to identify the load event which appended this record into the Enstar Conformed Model.",
	meta_ecmtransformdatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Enstar Conformed Model."
);
CREATE TABLE IF NOT EXISTS phinsys.transactiontype
(
	TransactionTypeCode varchar(50) comment "Code associated with the type of transaction <Dictionary ID='123'/>",
	TransactionSubTypeCode varchar(50) comment "The code associated with the type of transaction. This is a subset of transaction type <Dictionary ID='125'/>",
	SourceSystemCode varchar(50),
	TransactionTypeDescription varchar(100) comment "Description associated with the type of transaction <Dictionary ID='124'/>",
	TransactionSubTypeDescription varchar(100) comment "The description associated with the type of transaction. This is a subset of transaction type <Dictionary ID='126'/>",
	Group varchar(50) comment "Allows for basic grouping of transactions, such as 'USM' or 'SCM'",
	IsCashTransactionType boolean comment "An indicator which identifies whether the transaction is a cash or non-cash transaction <Dictionary ID='3'/>",
	meta_sourcesystemtransactiondatetime timestamp comment "Metadata to identify the date and time that this record was updated on the source system. This should formatted as a JDBC-compliant java.sql.Timestamp:'YYYY-MM-DD HH:MM:SS.fffffffff'.",
	meta_cdcchangesequence varchar(35) comment "Metadata to record the sequence of the changes occurring on the source system.",
	meta_cdcloaddatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Source Data layer.",
	meta_cdcchangeoperation varchar(1) comment "Metadata to record the operation identified by CDC on the source system - one of Update, Insert or Delete.",
	meta_ecmtransformidentifier varchar(35) comment "Metadata to identify the load event which appended this record into the Enstar Conformed Model.",
	meta_ecmtransformdatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Enstar Conformed Model."
);
CREATE TABLE IF NOT EXISTS phinsys.trustfund
(
	TrustFundCode varchar(2),
	SourceSystemCode varchar(50),
	TrustFundCodeDescription varchar(255),
	meta_sourcesystemtransactiondatetime timestamp comment "Metadata to identify the date and time that this record was updated on the source system. This should formatted as a JDBC-compliant java.sql.Timestamp:'YYYY-MM-DD HH:MM:SS.fffffffff'.",
	meta_cdcchangesequence varchar(35) comment "Metadata to record the sequence of the changes occurring on the source system.",
	meta_cdcloaddatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Source Data layer.",
	meta_cdcchangeoperation varchar(1) comment "Metadata to record the operation identified by CDC on the source system - one of Update, Insert or Delete.",
	meta_ecmtransformidentifier varchar(35) comment "Metadata to identify the load event which appended this record into the Enstar Conformed Model.",
	meta_ecmtransformdatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Enstar Conformed Model."
);
CREATE TABLE IF NOT EXISTS phinsys.underwriter
(
	UnderwriterCode varchar(50) comment "The code associated with the underwriter that underwrote the risk",
	SourceSystemCode varchar(50) comment "The source system code associated with the underwriter record",
	UnderwriterName varchar(255) comment "The name associated with the underwriter that underwrote the risk",
	meta_sourcesystemtransactiondatetime timestamp comment "Metadata to identify the date and time that this record was updated on the source system. This should formatted as a JDBC-compliant java.sql.Timestamp:'YYYY-MM-DD HH:MM:SS.fffffffff'.",
	meta_cdcchangesequence varchar(35) comment "Metadata to record the sequence of the changes occurring on the source system.",
	meta_cdcloaddatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Source Data layer.",
	meta_cdcchangeoperation varchar(1) comment "Metadata to record the operation identified by CDC on the source system - one of Update, Insert or Delete.",
	meta_ecmtransformidentifier varchar(35) comment "Metadata to identify the load event which appended this record into the Enstar Conformed Model.",
	meta_ecmtransformdatetime timestamp comment "Metadata to identify the date and time this record was loaded into the Enstar Conformed Model."
);
