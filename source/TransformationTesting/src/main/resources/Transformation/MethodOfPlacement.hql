SELECT
    CAST(business_type AS STRING) AS methodofplacementcode,
    "NDEX" AS sourcesystemcode,
    type_description AS methodofplacementdescription
FROM
    lookup_business_type