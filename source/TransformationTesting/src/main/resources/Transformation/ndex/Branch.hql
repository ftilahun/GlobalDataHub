SELECT
    "NDEX" AS sourcesystemcode,
    ltrim(CASE regexp_extract(profit_centre_desc, '((?:Syndicate 1301)|(?:TIE)|(?:TIUK))(.*)', 2)
        WHEN '' THEN 'London'
        ELSE regexp_extract(profit_centre_desc, '((?:Syndicate 1301)|(?:TIE)|(?:TIUK))(.*)', 2)
    END) AS branchcode,
    ltrim(CASE regexp_extract(profit_centre_desc, '((?:Syndicate 1301)|(?:TIE)|(?:TIUK))(.*)', 2)
        WHEN '' THEN 'London'
        ELSE regexp_extract(profit_centre_desc, '((?:Syndicate 1301)|(?:TIE)|(?:TIUK))(.*)', 2)
    END) AS branchdescription,
    CAST(NULL AS STRING) AS branchlocationcode
FROM
    lookup_profit_centre