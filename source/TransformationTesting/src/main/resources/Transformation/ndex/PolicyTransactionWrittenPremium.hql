SELECT

    CONCAT(
        CAST(line.line_id AS STRING), "-",
        IF(line_risk_code.risk_code IS NOT NULL, line_risk_code.risk_code, "MISSING"), "-",
        IF(layer_trust_fund.trust_fund_indicator IS NOT NULL, layer_trust_fund.trust_fund_indicator, "MISSING")) AS transactionreference,
    "NDEX" AS sourcesystemcode,
    "NDEX" AS sourcesystemdescription,
    CAST(line.line_id AS STRING) AS coveragereference,
    false AS iscashtransactiontype,
    CAST(IF (line.business_type IN (1,17), 0,
        line.slip_income_amount * (line.reporting_line_pct / 100.00)  *
        (IF(line_risk_code.risk_code_pct IS NOT NULL, line_risk_code.risk_code_pct, CAST(100.00 AS DECIMAL(18,2))) / 100.00) *
        (IF(layer_trust_fund.est_premium_split_pct IS NOT NULL, layer_trust_fund.est_premium_split_pct, CAST(100.00 AS DECIMAL(5,2))) / 100.00) ) AS DECIMAL(18,6)) AS originalamount,
    layer.premium_ccy AS originalcurrencycode,
    line.risk_reference AS policynumber,
    CAST(line.layer_id AS STRING) AS sectionreference,
    CAST(IF(line.business_type IN (1,17), 0,
        (line.slip_income_amount / layer.premium_roe )* (line.reporting_line_pct / 100.00)  *
        (IF(line_risk_code.risk_code_pct IS NOT NULL, line_risk_code.risk_code_pct, CAST(100.00 AS DECIMAL(18,2))) / 100.00) *
        (IF(layer_trust_fund.est_premium_split_pct IS NOT NULL, layer_trust_fund.est_premium_split_pct, CAST(100.00 AS DECIMAL(5,2))) / 100.00) ) AS DECIMAL(18,6)) AS settlementamount,
    line.epi_settlement_ccy AS settlementcurrencycode,
    CAST(layer.inception_date AS STRING) AS transactiondate,
    "WrittenPremiumOurShare" AS transactiontypecode,
    "WrittenPremiumOurShare" AS transactiontypedescription,
    "WrittenPremiumOurShare" AS transactionsubtypecode,
    "WrittenPremiumOurShare" AS transactionsubtypedescription,
    line_risk_code.risk_code AS riskcode,
    layer.fil_code AS filcode,
    layer_trust_fund.trust_fund_indicator AS trustfundcode

FROM

    line
    INNER JOIN layer
    ON layer.layer_id = line.layer_id
    LEFT JOIN line_risk_code
    ON line.line_id = line_risk_code.line_id
    LEFT JOIN layer_trust_fund
    ON line.layer_id = layer_trust_fund.layer_id
