SELECT
    COUNT(CONCAT(
                  CAST(policyprem.policyid AS STRING),
                  "WrittenPremiumOurShare",
                  CAST(policyprem.policypremid AS STRING),
                  IF(rc.codevalue IS NOT NULL, rc.codevalue, "MISSING"),
                  IF(tf.codevalue IS NOT NULL, tf.codevalue, "MISSING")) ) AS recordcount
FROM
policyprem
    INNER JOIN policyline ON policyprem.policyid = policyline.policyid
    INNER JOIN policy ON policyline.policyid = policy.policyid
    LEFT JOIN objcode rc ON rc.parentid = policyline.policyid
    AND rc.codename = 'RiskCode' AND rc.parenttable = 'Policy'
    LEFT JOIN objcode tf ON tf.parentid = policyline.policyid
    AND tf.codename = 'TrustFundCode' AND tf.parenttable = 'Policy'
    LEFT JOIN policyendorsmnt ON policyline.policyid = policyendorsmnt.policyid