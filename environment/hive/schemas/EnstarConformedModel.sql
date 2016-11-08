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


CREATE INDEX IDX_branchlocationcode_sourcesystemcode ON TABLE branch(branchlocationcode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_causeoflosscode_sourcesystemcode ON TABLE claim(causeoflosscode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_claimstatuscode_sourcesystemcode ON TABLE claim(claimstatuscode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_losslocationcode_sourcesystemcode ON TABLE claim(losslocationcode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_losstypecode_sourcesystemcode ON TABLE claim(losstypecode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_policynumber_sectionreference_coveragereference_sourcesystemcode ON TABLE claim(policynumber, sectionreference, coveragereference, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_claimreference_sourcesystemcode ON TABLE claimtransaction(claimreference, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_claimantreference_sourcesystemcode ON TABLE claimtransaction(claimantreference, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_transactiontypecode_sourcesystemcode ON TABLE claimtransaction(transactiontypecode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_deductiontypecode ON TABLE deduction(deductiontypecode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_deductiontypecode_sourcesystemcode ON TABLE deduction(deductiontypecode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_policynumber_sectionreference_sourcesystemcode ON TABLE deduction(policynumber, sectionreference, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_policynumber_sectionreference_coveragereference_sourcesystemcode ON TABLE deduction(policynumber, sectionreference,, coveragereference, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_geographytypecode_sourcesystemcode ON TABLE geography(geographytypecode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_parentlegalentitycode_parentlegalentitysourcesystemcode ON TABLE legalentity(parentlegalentitycode, parentlegalentitysourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_limitexcesstypecode_sourcesystemcode ON TABLE limitexcess(limitexcesstypecode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_policynumber_sectionreference_coveragereference_sourcesystemcode ON TABLE limitexcess(policynumber, sectionreference, coveragereference, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_sourcesystemcode_branchcode ON TABLE policy(sourcesystemcode, branchcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_brokercode_sourcesystemcode ON TABLE policy(brokercode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_risklocationcode_sourcesystemcode ON TABLE policy(risklocationcode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_insureddomicilecode_sourcesystemcode ON TABLE policy(insureddomicilecode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_insuredcode_sourcesystemcode ON TABLE policy(insuredcode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_legalentitycode ON TABLE policy(legalentitycode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_legalentitycode_sourcesystemcode ON TABLE policy(legalentitycode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_lineofbusinesscode ON TABLE policy(lineofbusinesscode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_lineofbusinesscode_sourcesystemcode ON TABLE policy(lineofbusinesscode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_methodofplacementcode ON TABLE policy(methodofplacementcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_methodofplacementcode_sourcesystemcode ON TABLE policy(methodofplacementcode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_policystatuscode ON TABLE policy(policystatuscode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_policystatuscode_sourcesystemcode ON TABLE policy(policystatuscode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_policyeventtypecode ON TABLE policy(policyeventtypecode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_policyeventtypecode_sourcesystemcode ON TABLE policy(policyeventtypecode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_sourcesystemcode ON TABLE policy(sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_underwritercode_sourcesystemcode ON TABLE policy(underwritercode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_accountingcurrencycode_sourcesystemcode ON TABLE policytransaction(accountingcurrencycode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_originalcurrencycode_sourcesystemcode ON TABLE policytransaction(originalcurrencycode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_settlementcurrencycode_sourcesystemcode ON TABLE policytransaction(settlementcurrencycode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_policynumber_sectionreference_coveragereference_sourcesystemcode ON TABLE policytransaction(policynumber, sectionreference, coveragereference, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

CREATE INDEX IDX_transactiontypecode_sourcesystemcode ON TABLE policytransaction(transactiontypecode, sourcesystemcode)
AS 'COMPACT' WITH DEFERRED REBUILD STORED AS Avro;

