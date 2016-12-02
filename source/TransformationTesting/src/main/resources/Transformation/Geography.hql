SELECT
    CONCAT("[Missing][Missing][Missing][", country_code, "]") AS geographycode,
    "NDEX" AS sourcesystemcode,
    "Missing" AS zipcode,
    "Missing" AS city,
    "Missing" AS statecode,
    "Missing" AS statedescription,
    country_code AS countrycode,
    country_name AS countrydescription
FROM
    lookup_country