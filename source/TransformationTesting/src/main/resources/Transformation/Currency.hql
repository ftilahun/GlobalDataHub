SELECT
    CAST(currency_code as STRING) AS currencycode,
    "NDEX" AS sourcesystemcode,
    CAST(currency_desc as STRING) AS description
FROM
    lookup_currency