SELECT
    "NDEX" AS sourcesystemcode,
    CAST(profit_centre_code AS STRING) AS branchcode,
    profit_centre_desc AS branchdescription,
    CAST(NULL AS STRING) AS branchlocationcode
FROM
    lookup_profit_centre