SELECT DISTINCT
    regexp_extract(profit_centre_desc, '(^(TIE)|(TIUK)|(Syndicate 1301))', 0) AS legalentitycode,
    "NDEX" AS sourcesystemcode,
    "NDEX" AS sourcesystemdescription,
    regexp_extract(profit_centre_desc, '(^(TIE)|(TIUK)|(Syndicate 1301))', 0) as legalentitydescription,
    CAST(NULL AS STRING) AS parentlegalentitycode,
    CAST(NULL AS STRING) AS parentlegalentitysourcesystemcode,
    false AS isconformed
FROM
    lookup_profit_centre
