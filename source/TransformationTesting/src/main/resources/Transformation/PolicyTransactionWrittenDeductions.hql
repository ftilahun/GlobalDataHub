SELECT
    CONCAT(
        CAST(layer.layer_id AS STRING), "-",
        CAST(line.line_id AS STRING), "-",
        CAST(layer_deduction.deduction_id AS STRING), "-",
        IF(line_risk_code.risk_code IS NOT NULL, line_risk_code.risk_code, "MISSING"), "-",
        IF(layer_trust_fund.trust_fund_indicator IS NOT NULL, layer_trust_fund.trust_fund_indicator, "MISSING")) AS transactionreference,
    "NDEX" AS sourcesystemcode,
    "NDEX" AS sourcesystemdescription,
    CAST(line.line_id AS STRING) AS coveragereference,
    false AS iscashtransactiontype,
    CAST(
        IF (line.business_type IN (1,17), 0,
        (line.slip_income_amount * (line.reporting_line_pct / 100) ) *
        ((deductiontmptable.net_pct/100) * (layer_deduction.deduction_pct/100) *
        (IF(line_risk_code.risk_code_pct IS NOT NULL, line_risk_code.risk_code_pct, CAST(100.00 AS DECIMAL(18,2))) / 100) *
        (IF(layer_trust_fund.est_premium_split_pct IS NOT NULL, layer_trust_fund.est_premium_split_pct, CAST(100.00 AS DECIMAL(5,2))) / 100))) AS DECIMAL(18,6)) AS originalamount,
    CAST(layer.premium_ccy AS STRING) AS originalcurrencycode,
    CAST(line.risk_reference AS STRING) AS policynumber,
    CAST(line.layer_id AS STRING) AS sectionreference,
    CAST(
        IF (line.business_type IN (1,17), 0,
        (line.slip_income_amount / layer.premium_roe * (line.reporting_line_pct / 100) ) *
        ((deductiontmptable.net_pct/100) * (layer_deduction.deduction_pct/100) *
        (IF(line_risk_code.risk_code_pct IS NOT NULL, line_risk_code.risk_code_pct, CAST(100.00 AS DECIMAL(18,2))) / 100) *
        (IF(layer_trust_fund.est_premium_split_pct IS NOT NULL, layer_trust_fund.est_premium_split_pct, CAST(100.00 AS DECIMAL(5,2))) / 100))) AS DECIMAL(18,6)) AS settlementamount,
    CAST(line.epi_settlement_ccy AS STRING) AS settlementcurrencycode,
    CAST(layer.inception_date AS STRING) AS transactiondate,
    "WrittenDeductionsOurShare" AS transactiontypecode,
    "WrittenDeductionsOurShare" AS transactiontypedescription,
    layer_deduction.deduction_code AS transactionsubtypecode,
    lookup_deduction_type.deduction_description AS transactionsubtypedescription,
    layer.FIL_code AS filcode,
    CAST(line_risk_code.risk_code AS STRING) AS riskcode,
    CAST(layer_trust_fund.trust_fund_indicator AS STRING) AS trustfundcode
FROM
    line
    LEFT JOIN line_risk_code
    ON line.line_id = line_risk_code.line_id
    LEFT JOIN layer_trust_fund
    ON line.layer_id = layer_trust_fund.layer_id
    INNER JOIN layer_deduction
    ON line.layer_id = layer_deduction.layer_id
    LEFT JOIN lookup_deduction_type
    ON layer_deduction.deduction_code = lookup_deduction_type.deduction_code
    INNER JOIN layer
    ON line.layer_id = layer.layer_id
    INNER JOIN (SELECT
             ld1.deduction_id,
             ld1.layer_id AS layer_id,
             CAST(net_as_pct_of_gross(ld2.sequence_no,CAST(ld2.deduction_pct AS DECIMAL(18,6))) AS STRING) AS net_pct
          FROM layer_deduction ld1
          LEFT JOIN layer_deduction ld2
             ON ld1.layer_id = ld2.layer_id
             AND ld2.sequence_no < ld1.sequence_no
          GROUP BY ld1.deduction_id, ld1.layer_id) AS deductiontmptable
    ON deductiontmptable.deduction_id = layer_deduction.deduction_id