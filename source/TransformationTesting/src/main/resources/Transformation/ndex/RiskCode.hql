SELECT
    CAST(risk_code AS STRING) AS riskcode,
    "NDEX" AS sourcesystemcode,
    risk_code_desc AS riskcodedescription
FROM
    lookup_risk_code