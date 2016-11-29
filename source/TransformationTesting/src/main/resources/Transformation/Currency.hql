SELECT
    CAST(currency_code AS STRING) AS currencycode,
    "NDEX" AS sourcesystemcode,
    CAST(currency_desc AS STRING) AS currencydescription
FROM
    lookup_currency