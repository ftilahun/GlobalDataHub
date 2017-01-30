SELECT
    SUBSTR(IADDCD, 0, 3) AS transactionsubtypecode,
    IADDDS AS transactionsubtypecodedescription,
    "WrittenDeductionsOurShare" AS transactiontypecode,
    "GENIUS" AS sourcesystemcode,
    "WrittenDeductionsOurShare" AS transactiontypedescription,
    "Deduction" AS group,
    false AS iscashtransactiontype
FROM
    ICDCREP