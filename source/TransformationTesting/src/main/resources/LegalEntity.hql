SELECT
    CAST(profit_centre_code as string) as legalentitycode,
    "NDEX" as sourcesystemcode,
    profit_centre_desc as legalentitydescription,
    CAST(NULL as string) as parentlegalentitycode,
    CAST(NULL as string) as parentlegalentitysourcesystemcode
FROM
    lookup_profit_centre