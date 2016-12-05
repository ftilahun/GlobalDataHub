SELECT
    CAST(profit_centre_code AS STRING) AS legalentitycode,
    "NDEX" AS sourcesystemcode,
    regexp_extract(profit_centre_desc, '(^(TIE)|(TIUK)|(Syndicate 1301))', 0) as legalentitydescription,
    CAST(NULL AS STRING) AS parentlegalentitycode,
    CAST(NULL AS STRING) AS parentlegalentitysourcesystemcode,
    false AS isconformed,
    "NDEX" AS sourcesystemdescription
FROM
    lookup_profit_centre
