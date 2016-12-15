SELECT
    line.risk_reference AS policynumber,
    CAST(line.layer_id AS STRING) AS sectionreference,
    CAST(line_risk_code.line_id AS STRING) AS coveragereference,
    "NDEX" AS sourcesystemcode,
    "RiskCode" AS analysiscodetype,
    line_risk_code.risk_code AS analysiscode,
    lookup_risk_code.risk_code_desc AS analysiscodedescription,
    line_risk_code.risk_code_pct AS splitpercent
FROM line
    JOIN line_risk_code
    ON line.line_id = line_risk_code.line_id
    JOIN lookup_risk_code
    ON line_risk_code.risk_code = lookup_risk_code.risk_code