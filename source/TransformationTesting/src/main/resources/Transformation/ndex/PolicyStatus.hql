SELECT
    line_status AS policystatuscode,
    "NDEX" AS sourcesystemcode,
    status_desc AS policystatusdescription
FROM
    lookup_line_status