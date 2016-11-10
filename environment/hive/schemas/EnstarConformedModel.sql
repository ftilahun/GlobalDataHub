/* Create Schema */

CREATE SCHEMA IF NOT EXISTS ecm

/* Create Tables */

CREATE EXTERNAL TABLE IF NOT EXISTS ecm.branch 
STORED AS AVRO
LOCATION '/datahub/ecm/branch'
TBLPROPERTIES ('avro.schema.url'='/metadata/datahub/ecm/branch/branch.avsc');

CREATE EXTERNAL TABLE IF NOT EXISTS ecm.broker
STORED AS AVRO
LOCATION '/datahub/ecm/broker'
TBLPROPERTIES ('avro.schema.url'='/metadata/datahub/ecm/broker/broker.avsc');

CREATE EXTERNAL TABLE IF NOT EXISTS ecm.causeofloss
STORED AS AVRO
LOCATION '/datahub/ecm/causeofloss'
TBLPROPERTIES ('avro.schema.url'='/metadata/datahub/ecm/causeofloss/causeofloss.avsc');

CREATE EXTERNAL TABLE IF NOT EXISTS ecm.claim
STORED AS AVRO
LOCATION '/datahub/ecm/claim'
TBLPROPERTIES ('avro.schema.url'='/metadata/datahub/ecm/claim/claim.avsc');

CREATE EXTERNAL TABLE IF NOT EXISTS ecm.claimant
STORED AS AVRO
LOCATION '/datahub/ecm/claimant'
TBLPROPERTIES ('avro.schema.url'='/metadata/datahub/ecm/claimant/claimant.avsc');

CREATE EXTERNAL TABLE IF NOT EXISTS ecm.claimstatus
STORED AS AVRO
LOCATION '/datahub/ecm/claimstatus'
TBLPROPERTIES ('avro.schema.url'='/metadata/datahub/ecm/claimstatus/claimstatus.avsc');

CREATE EXTERNAL TABLE IF NOT EXISTS ecm.claimtransaction
STORED AS AVRO
LOCATION '/datahub/ecm/claimtransaction'
TBLPROPERTIES ('avro.schema.url'='/metadata/datahub/ecm/claimtransaction/claimtransaction.avsc');

CREATE EXTERNAL TABLE IF NOT EXISTS ecm.currency
STORED AS AVRO
LOCATION '/datahub/ecm/currency'
TBLPROPERTIES ('avro.schema.url'='/metadata/datahub/ecm/currency/currency.avsc');

CREATE EXTERNAL TABLE IF NOT EXISTS ecm.deduction
STORED AS AVRO
LOCATION '/datahub/ecm/deduction'
TBLPROPERTIES ('avro.schema.url'='/metadata/datahub/ecm/deduction/deduction.avsc');

CREATE EXTERNAL TABLE IF NOT EXISTS ecm.deductiontype
STORED AS AVRO
LOCATION '/datahub/ecm/deductiontype'
TBLPROPERTIES ('avro.schema.url'='/metadata/datahub/ecm/deductiontype/deductiontype.avsc');

CREATE EXTERNAL TABLE IF NOT EXISTS ecm.geography
STORED AS AVRO
LOCATION '/datahub/ecm/geography'
TBLPROPERTIES ('avro.schema.url'='/metadata/datahub/ecm/geography/geography.avsc');

CREATE EXTERNAL TABLE IF NOT EXISTS ecm.geographytype
STORED AS AVRO
LOCATION '/datahub/ecm/geographytype'
TBLPROPERTIES ('avro.schema.url'='/metadata/datahub/ecm/geographytype/geographytype.avsc');

CREATE EXTERNAL TABLE IF NOT EXISTS ecm.insured
STORED AS AVRO
LOCATION '/datahub/ecm/insured'
TBLPROPERTIES ('avro.schema.url'='/metadata/datahub/ecm/insured/insured.avsc');

CREATE EXTERNAL TABLE IF NOT EXISTS ecm.legalentity
STORED AS AVRO
LOCATION '/datahub/ecm/legalentity'
TBLPROPERTIES ('avro.schema.url'='/metadata/datahub/ecm/legalentity/legalentity.avsc');

CREATE EXTERNAL TABLE IF NOT EXISTS ecm.limitexcess
STORED AS AVRO
LOCATION '/datahub/ecm/limitexcess'
TBLPROPERTIES ('avro.schema.url'='/metadata/datahub/ecm/limitexcess/limitexcess.avsc');

CREATE EXTERNAL TABLE IF NOT EXISTS ecm.limitexcesstype
STORED AS AVRO
LOCATION '/datahub/ecm/limitexcesstype'
TBLPROPERTIES ('avro.schema.url'='/metadata/datahub/ecm/limitexcesstype/limitexcesstype.avsc');

CREATE EXTERNAL TABLE IF NOT EXISTS ecm.lineofbusiness
STORED AS AVRO
LOCATION '/datahub/ecm/lineofbusiness'
TBLPROPERTIES ('avro.schema.url'='/metadata/datahub/ecm/lineofbusiness/lineofbusiness.avsc');

CREATE EXTERNAL TABLE IF NOT EXISTS ecm.losstype
STORED AS AVRO
LOCATION '/datahub/ecm/losstype'
TBLPROPERTIES ('avro.schema.url'='/metadata/datahub/ecm/losstype/losstype.avsc');

CREATE EXTERNAL TABLE IF NOT EXISTS ecm.methodofplacement
STORED AS AVRO
LOCATION '/datahub/ecm/methodofplacement'
TBLPROPERTIES ('avro.schema.url'='/metadata/datahub/ecm/methodofplacement/methodofplacement.avsc');

CREATE EXTERNAL TABLE IF NOT EXISTS ecm.policy
STORED AS AVRO
LOCATION '/datahub/ecm/policy'
TBLPROPERTIES ('avro.schema.url'='/metadata/datahub/ecm/policy/policy.avsc');

CREATE EXTERNAL TABLE IF NOT EXISTS ecm.policyeventtype
STORED AS AVRO
LOCATION '/datahub/ecm/policyeventtype'
TBLPROPERTIES ('avro.schema.url'='/metadata/datahub/ecm/policyeventtype/policyeventtype.avsc');

CREATE EXTERNAL TABLE IF NOT EXISTS ecm.policystatus
STORED AS AVRO
LOCATION '/datahub/ecm/policystatus'
TBLPROPERTIES ('avro.schema.url'='/metadata/datahub/ecm/policystatus/policystatus.avsc');

CREATE EXTERNAL TABLE IF NOT EXISTS ecm.policytransaction
STORED AS AVRO
LOCATION '/datahub/ecm/policytransaction'
TBLPROPERTIES ('avro.schema.url'='/metadata/datahub/ecm/policytransaction/policytransaction.avsc');

CREATE EXTERNAL TABLE IF NOT EXISTS ecm.sourcesystem
STORED AS AVRO
LOCATION '/datahub/ecm/sourcesystem'
TBLPROPERTIES ('avro.schema.url'='/metadata/datahub/ecm/sourcesystem/sourcesystem.avsc');

CREATE EXTERNAL TABLE IF NOT EXISTS ecm.transactiontype
STORED AS AVRO
LOCATION '/datahub/ecm/transactiontype'
TBLPROPERTIES ('avro.schema.url'='/metadata/datahub/ecm/transactiontype/transactiontype.avsc');

CREATE EXTERNAL TABLE IF NOT EXISTS ecm.underwriter
STORED AS AVRO
LOCATION '/datahub/ecm/underwriter'
TBLPROPERTIES ('avro.schema.url'='/metadata/datahub/ecm/underwriter/underwriter.avsc');

/* Create indexes */
/* Note: The index needs to be rebuilt/statistics recomputed with each new partition */

CREATE INDEX IDX_Branch ON TABLE ecm.branch(branchcode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_Branch_Geography ON TABLE ecm.branch(branchlocationcode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_Broker ON TABLE ecm.broker(brokercode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_CauseOfLoss ON TABLE ecm.causeofloss(causeoflosscode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_Claim ON TABLE ecm.claim(claimreference, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_Claim_CauseOfLoss ON TABLE ecm.claim(causeoflosscode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_Claim_ClaimStatus ON TABLE ecm.claim(claimstatuscode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_Claim_Geography ON TABLE ecm.claim(losslocationcode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_Claim_LossType ON TABLE ecm.claim(losstypecode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_Claim_Policy ON TABLE ecm.claim(policynumber, sectionreference, coveragereference, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_Claimant ON TABLE ecm.claimant(claimantreference, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_ClaimStatus ON TABLE ecm.claimstatus(claimstatuscode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_ClaimTransaction ON TABLE ecm.claimtransaction(transactionreference, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_ClaimTransaction_Claim ON TABLE ecm.claimtransaction(claimreference, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_ClaimTransaction_Claimant ON TABLE ecm.claimtransaction(claimantreference, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_ClaimTransaction_TransactionType ON TABLE ecm.claimtransaction(transactiontypecode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_Currency ON TABLE ecm.currency(currencycode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_Deduction ON TABLE ecm.deduction(deductionreference)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_Deduction_DeductionType ON TABLE ecm.deduction(deductiontypecode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_Deduction_DeductionType_02 ON TABLE ecm.deduction(deductiontypecode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_Deduction_Policy ON TABLE ecm.deduction(policynumber, sectionreference, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_Deduction_Policy_02 ON TABLE ecm.deduction(policynumber, sectionreference,, coveragereference, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_DeductionType ON TABLE ecm.deductiontype(deductiontypecode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_Geography ON TABLE ecm.geography(geographycode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_Geography_GeographyType ON TABLE ecm.geography(geographytypecode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_GeographyType ON TABLE ecm.geographytype(geographytypecode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_Insured ON TABLE ecm.insured(insuredcode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_LegalEntity ON TABLE ecm.legalentity(legalentitycode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_LegalEntity_LegalEntity ON TABLE ecm.legalentity(parentlegalentitycode, parentlegalentitysourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_LimitExcess ON TABLE ecm.limitexcess(limitexcessreference, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_LimitExcess_LimitExcessType ON TABLE ecm.limitexcess(limitexcesstypecode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_LimitExcess_Policy ON TABLE ecm.limitexcess(policynumber, sectionreference, coveragereference, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_LimitExcessType ON TABLE ecm.limitexcesstype(limitexcesstypecode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_LineOfBusiness ON TABLE ecm.lineofbusiness(lineofbusinesscode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_LossType ON TABLE ecm.losstype(losstypecode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_MethodOfPlacement ON TABLE ecm.methodofplacement(methodofplacementcode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_Policy ON TABLE ecm.policy(policynumber, sectionreference, coveragereference, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_Policy_Branch ON TABLE ecm.policy(sourcesystemcode, branchcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_Policy_Broker ON TABLE ecm.policy(brokercode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_Policy_Geography ON TABLE ecm.policy(risklocationcode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_Policy_Geography_02 ON TABLE ecm.policy(insureddomicilecode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_Policy_Insured ON TABLE ecm.policy(insuredcode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_Policy_LegalEntity ON TABLE ecm.policy(legalentitycode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_Policy_LegalEntity_02 ON TABLE ecm.policy(legalentitycode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_Policy_LineOfBusiness ON TABLE ecm.policy(lineofbusinesscode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_Policy_LineOfBusiness_02 ON TABLE ecm.policy(lineofbusinesscode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_Policy_MethodOfPlacement ON TABLE ecm.policy(methodofplacementcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_Policy_MethodOfPlacement_02 ON TABLE ecm.policy(methodofplacementcode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_Policy_PolicyStatus ON TABLE ecm.policy(policystatuscode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_Policy_PolicyStatus_02 ON TABLE ecm.policy(policystatuscode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_Policy_PolicyType ON TABLE ecm.policy(policyeventtypecode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_Policy_PolicyType_02 ON TABLE ecm.policy(policyeventtypecode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_Policy_SourceSystem ON TABLE ecm.policy(sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_Policy_Underwriter ON TABLE ecm.policy(underwritercode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_PolicyEventType ON TABLE ecm.policyeventtype(policyeventtypecode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_PolicyStatus ON TABLE ecm.policystatus(policystatuscode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_PolicyTransaction ON TABLE ecm.policytransaction(transactionreference, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_Transaction_Currency_Accounting ON TABLE ecm.policytransaction(accountingcurrencycode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_Transaction_Currency_Original ON TABLE ecm.policytransaction(originalcurrencycode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_Transaction_Currency_Settlement ON TABLE ecm.policytransaction(settlementcurrencycode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_Transaction_Policy ON TABLE ecm.policytransaction(policynumber, sectionreference, coveragereference, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_Transaction_TransactionType ON TABLE ecm.policytransaction(transactiontypecode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_SourceSystem ON TABLE ecm.sourcesystem(sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_TransactionType ON TABLE ecm.transactiontype(transactiontypecode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_Underwriter ON TABLE ecm.underwriter(underwritercode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

