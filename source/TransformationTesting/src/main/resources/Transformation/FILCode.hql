SELECT
    CAST(fil_code AS STRING) AS filcode,
    "NDEX" AS sourcesystemcode,
    fil_description AS filcodedescription
FROM
    lookup_fil_code