SELECT
    "NDEX" AS sourcesystemcode,
    broking_company_name AS brokerdescription,
    CONCAT(CAST(broking_company_code AS string), "-", broking_company_pseudonym) AS brokercode
FROM
    broking_company
