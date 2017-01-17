SELECT
    line.risk_reference AS policynumber,
    CAST(layer_trust_fund.layer_id AS STRING) AS sectionreference,
    CAST(line.line_id AS STRING) AS coveragereference,
    "NDEX" AS sourcesystemcode,
    "TrustFund" AS analysiscodetype,
    layer_trust_fund.trust_fund_indicator AS analysiscode,
    lookup_trust_fund.fund_description AS analysiscodedescription,
    CAST(layer_trust_fund.est_premium_split_pct AS DECIMAL(12,7)) AS splitpercent
FROM
    line JOIN layer_trust_fund
    ON line.layer_id = layer_trust_fund.layer_id
    JOIN lookup_trust_fund
    ON layer_trust_fund.trust_fund_indicator = lookup_trust_fund.trust_fund_indicator