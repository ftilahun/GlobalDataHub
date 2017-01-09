SELECT
    COUNT(policynumber) AS recordcount
FROM
    analysiscodesplit
WHERE
    analysiscodetype = "TrustFund"