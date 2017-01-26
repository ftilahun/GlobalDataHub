SELECT
    COUNT(transactionsubtypecode) AS recordcount
FROM transactiontype
WHERE transactiontypecode = "WrittenDeductionsOurShare"