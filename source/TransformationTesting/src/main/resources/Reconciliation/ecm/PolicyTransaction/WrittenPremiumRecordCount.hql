SELECT
    COUNT(transactionreference) AS recordcount
FROM policytransaction
WHERE transactiontypecode = "WrittenPremiumOurShare"
