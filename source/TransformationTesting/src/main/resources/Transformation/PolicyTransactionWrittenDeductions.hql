SELECT
    CONCAT(layer_id, '-', endorsement_id, '-', row_number, '-', risk_code, '-', trust_fund_indicator, '-', deduction_id) AS transactionreference,
    AS sourcesystemcode,
    AS policynumber,
    AS sectionreference,
    AS coveragereference,
    AS filcode,
    AS riskcode,
    AS transactiontypecode,
    AS transactionsubtypecode,
    AS trustfundcode,
    AS originalcurrencycode,
    AS settlementcurrencycode,
    AS accountingcurrencycode,
    AS settlementrateofexchange,
    AS accountingrateofexchange,
    AS originalamount,
    AS settlementamount,
    AS accountingamount,
    AS accountingperiod,
    AS transactiondate
FROM
    line
    JOIN settlement_schedule
    ON line.layer_id = settlement_schedule.layer_id
    JOIN Line_Risk_Code
    ON line.line_id = Line_Risk_Code.Line_Id
    JOIN layer_trust_fund
    ON line.layer_id = layer_trust_fund.layer_id
    JOIN layer_deduction
    ON line.layer_id = layer_deduction.layer_id

    JOIN lookup_risk_code
    ON Line_Risk_Code.Risk_Code = lookup_risk_code.RISK_CODE

    JOIN lookup_deduction_type
    ON layer_deduction.deduction_code = lookup_deduction_type.deduction_code

    JOIN lookup_trust_fund
    ON layer_trust_fund.trust_fund_indicator = lookup_trust_fund.trust_fund_indicator


//IsCashTransactionType
//    OriginalCurrencyCode
//    SectionReference
//    TransactionTypeDescription
//    TransactionSubTypeDescription