SELECT
    "NDEX" AS sourcesystemcode,
    CAST(profit_centre_code as string) AS branchcode,
    profit_centre_desc AS branchdescription,
    CAST(NULL as string) AS branchlocationcode
FROM
    lookup_profit_centre