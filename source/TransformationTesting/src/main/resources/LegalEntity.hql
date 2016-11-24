SELECT
    CAST(profit_centre_code AS STRING) AS legalentitycode,
    "NDEX" AS sourcesystemcode,
    profit_centre_desc AS legalentitydescription,
    CAST(NULL AS STRING) AS parentlegalentitycode,
    CAST(NULL AS STRING) AS parentlegalentitysourcesystemcode
FROM
    lookup_profit_centre