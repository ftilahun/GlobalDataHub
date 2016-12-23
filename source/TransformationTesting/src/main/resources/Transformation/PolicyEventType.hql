SELECT
    CAST(renewal_status_id AS STRING) AS policyeventtypecode,
    "NDEX" AS sourcesystemcode,
    renewal_status_desc AS policyeventtypedescription
FROM
    lookup_renewal_status