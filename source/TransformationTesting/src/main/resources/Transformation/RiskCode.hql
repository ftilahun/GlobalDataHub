SELECT
    CAST(risk_code AS STRING) AS riskcode,
    "NDEX" AS sourcesystemcode,
    CAST(risk_code_desc AS STRING) AS riskcodedescription
FROM
    lookup_risk_code