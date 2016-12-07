SELECT

    CONCAT(settlement_schedule.layer_id, "-",
        settlement_schedule.endorsement_id, "-",
        settlement_schedule.row_number, "-",
        line_risk_code.risk_code, "-",
        layer_trust_fund.trust_fund_indicator) AS transactionreference,
    "NDEX" AS sourcesystemcode,
    CAST(line.line_id AS STRING) AS coveragereference,
    false AS iscashtransactiontype,
    CAST(
        IF (line.business_type IN (1,17), 0,
        settlement_schedule.amount * (line.reporting_line_pct / 100)  *
        line_risk_code.risk_code_pct * layer_trust_fund.est_premium_split_pct) AS STRING) AS originalamount,
    layer.premium_ccy AS originalcurrencycode,
    line.risk_reference AS policynumber,
    CAST(line.layer_id AS STRING) AS sectionreference,
    CAST(
        IF (line.business_type IN (1,17), 0,
        settlement_schedule.amount / layer.premium_roe * (line.reporting_line_pct / 100)  *
        line_risk_code.risk_code_pct * layer_trust_fund.est_premium_split_pct) AS STRING) AS settlementamount,
    line.epi_settlement_ccy AS settlementcurrencycode,
    settlement_schedule.settlement_due_date AS transactiondate,
    "WrittenPremiumOurShare" AS transactiontypecode,
    "WrittenPremiumOurShare"AS transactiontypedescription,
    lookup_premium_type.premium_type_code AS transactionsubtypecode,
    lookup_premium_type.premium_type_desc AS transactionsubtypedescription,
    line_risk_code.risk_code AS riskcode,
    layer.fil_code AS filcode,
    layer_trust_fund.trust_fund_indicator AS trustfundcode

FROM

    line
    JOIN layer
    ON layer.layer_id = line.layer_id
    JOIN settlement_schedule
    ON line.layer_id = settlement_schedule.layer_id
    JOIN line_risk_code
    ON line.line_id = line_risk_code.line_id
    JOIN layer_trust_fund
    ON line.layer_id = layer_trust_fund.layer_id
    LEFT JOIN lookup_premium_type
    ON lookup_premium_type.premium_type_code = settlement_schedule.premium_type_code