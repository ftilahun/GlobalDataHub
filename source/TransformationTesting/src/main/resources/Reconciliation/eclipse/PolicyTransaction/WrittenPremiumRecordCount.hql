SELECT
    COUNT(CONCAT(
                  CAST(policyprem.policyid AS STRING),
                  "WrittenPremiumOurShare",
                  CAST(policyprem.policypremid AS STRING),
                  IF(objcode1.codevalue IS NOT NULL, objcode1.codevalue, "MISSING"),
                  IF(objcode2.codevalue IS NOT NULL, objcode2.codevalue, "MISSING")) ) AS recordcount
FROM
policyprem
    INNER JOIN policyline ON policyprem.policyid = policyline.policyid
    INNER JOIN policy ON policyline.policyid = policy.policyid
    LEFT JOIN objcode objcode1 ON objcode1.parentid = policyline.policyid
    AND objcode1.codename = 'RiskCode' AND objcode1.parenttable = 'Policy'
    LEFT JOIN objcode objcode2 ON objcode2.parentid = policyline.policyid
    AND objcode2.codename = 'TrustFundCode' AND objcode2.parenttable = 'Policy'
    LEFT JOIN policyendorsmnt ON policyline.policyid = policyendorsmnt.policyid