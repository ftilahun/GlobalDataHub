SELECT
    "NDEX" as sourcesystemcode,
    CAST(profit_centre_code as string) as branchcode,
    profit_centre_desc as branchdescription,
    CAST(NULL as string) as branchlocationcode
FROM
    lookup_profit_centre