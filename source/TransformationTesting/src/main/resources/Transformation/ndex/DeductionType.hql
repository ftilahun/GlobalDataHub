SELECT
    deduction_code AS deductiontypecode,
    "NDEX" AS sourcesystemcode,
    deduction_description AS deductiontypedescription,
    CAST(reporting_group AS String) AS group
FROM
    lookup_deduction_type
