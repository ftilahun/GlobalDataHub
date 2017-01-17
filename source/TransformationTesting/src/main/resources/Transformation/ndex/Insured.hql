SELECT
    CAST(organisation_id AS STRING) AS insuredcode,
    "NDEX" AS sourcesystemcode,
    organisation_name AS insuredlegalname,
    organisation_name AS insuredname
FROM
    organisation