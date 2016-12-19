SELECT
    CAST(CONCAT(settlement_schedule.layer_id, '-', settlement_schedule.endorsement_id, '-', settlement_schedule.row_number, '-', line_risk_code.risk_code, '-', layer_trust_fund.trust_fund_indicator, '-', layer_deduction.deduction_id) AS STRING) AS transactionreference,
    "NDEX" AS sourcesystemcode,
    CAST(line.line_id AS STRING) AS coveragereference,
    false AS iscashtransactiontype,
    CAST(
        IF (line.business_type IN (1,17), 0,
        (settlement_schedule.amount * (line.reporting_line_pct / 100) ) * ((deductiontmptable.net_pct/100) * line_risk_code.risk_code_pct * layer_trust_fund.est_premium_split_pct))
        AS STRING) AS originalamount,
    CAST(layer.premium_ccy AS STRING) AS originalcurrencycode,
    CAST(line.risk_reference AS STRING) AS policynumber,
    CAST(line.layer_id AS STRING) AS sectionreference,
    CAST(
        IF (line.business_type IN (1,17), 0,
        (settlement_schedule.amount / layer.premium_roe * (line.reporting_line_pct / 100) ) * ((deductiontmptable.net_pct/100) * line_risk_code.risk_code_pct * layer_trust_fund.est_premium_split_pct))
        AS STRING) AS settlementamount,
    CAST(line.epi_settlement_ccy AS STRING) AS settlementcurrencycode,
    CAST(settlement_schedule.settlement_due_date AS STRING) AS transactiondate,
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
    JOIN line_risk_code
    ON line.line_id = line_risk_code.line_id
    JOIN layer_trust_fund
    ON line.layer_id = layer_trust_fund.layer_id
    JOIN layer_deduction
    ON line.layer_id = layer_deduction.layer_id
    JOIN lookup_risk_code
    ON line_risk_code.risk_code = lookup_risk_code.risk_code
    LEFT JOIN lookup_deduction_type
    ON layer_deduction.deduction_code = lookup_deduction_type.deduction_code
    JOIN lookup_trust_fund
    ON layer_trust_fund.trust_fund_indicator = lookup_trust_fund.trust_fund_indicator
    JOIN layer
    ON line.layer_id = layer.layer_id
    JOIN (SELECT
             ld1.deduction_id,
             ld1.layer_id AS layer_id,
             CAST(net_as_pct_of_gross(ld2.sequence_no,CAST(ld2.deduction_pct AS DECIMAL(10,2))) AS STRING) AS net_pct
          FROM layer_deduction ld1
          LEFT JOIN layer_deduction ld2
             ON ld1.layer_id = ld2.layer_id
             AND ld2.sequence_no < ld1.sequence_no
          GROUP BY ld1.deduction_id, ld1.layer_id) AS deductiontmptable
    ON deductiontmptable.deduction_id = layer_deduction.deduction_id
