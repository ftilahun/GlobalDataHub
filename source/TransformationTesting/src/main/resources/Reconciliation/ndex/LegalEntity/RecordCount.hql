SELECT
    COUNT(DISTINCT regexp_extract(profit_centre_desc, '(^(TIE)|(TIUK)|(Syndicate 1301))', 0)) AS recordcount
FROM lookup_profit_centre