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
    LEFT JOIN objcode rc ON rc.parentid = policyline.policyid
    AND rc.codename = 'RiskCode' AND rc.parenttable = 'Policy'
    LEFT JOIN objcode tf ON tf.parentid = policyline.policyid
    AND tf.codename = 'TrustFundCode' AND tf.parenttable = 'Policy'
    LEFT JOIN policyendorsmnt ON policyline.policyid = policyendorsmnt.policyid