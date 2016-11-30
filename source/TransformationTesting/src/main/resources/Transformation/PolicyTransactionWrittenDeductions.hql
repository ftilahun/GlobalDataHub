SELECT
    CAST(CONCAT(settlement_schedule.layer_id, '-', settlement_schedule.endorsement_id, '-', settlement_schedule.row_number, '-', line_risk_code.risk_code, '-', layer_trust_fund.trust_fund_indicator, '-', layer_deduction.deduction_id) AS STRING) AS transactionreference,
    "NDEX" AS sourcesystemcode,
    CAST(line.line_id AS STRING) AS coveragereference,
    false AS iscashtransactiontype,
    CASE line.business_type
        WHEN IN (1,17) THEN CAST(0 AS STRING)
        ELSE CAST((settlement_schedule.amount * (line.reporting_line_pct / 100) * deduction%) AS STRING)
        END AS originalamount,
    CAST(layer.premium_ccy AS STRING) AS originalcurrencycode,
    CAST(line.risk_reference AS STRING) AS policynumber,
    CAST(line.layer_id AS STRING) AS sectionreference,
    CASE line.business_type
        WHEN IN (1,17) THEN CAST(0 AS STRING)
        ELSE CAST((settlement_schedule.amount / layer.premium_roe * (line.reporting_line_pct / 100) * deduction%) AS STRING)
        END AS settlementamount,
    CAST(line.epi_settlement_ccy AS STRING) AS settlementcurrencycode,
    CAST(settlement_schedule.settlement_due_date AS STRING) AS transactiondate
    "WrittenDeductionsOurShare" AS transactiontypecode,
    "WrittenDeductionsOurShare" AS transactiontypedescription,
    layer_deduction.deduction_code AS transactionsubtypecode,
    lookup_deduction_type.deduction_description AS transactionsubtypedescription,
    layer.FIL_code AS filcode,
    CAST(line_risk_code.risk_code AS STRING) AS riskcode,
    CAST(layer_trust_fund.trust_fund_indicator AS STRING) AS trustfundcode
FROM
    line
    JOIN settlement_schedule
    ON line.layer_id = settlement_schedule.layer_id
    JOIN Line_Risk_Code
    ON line.line_id = line_risk_code.line_id
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