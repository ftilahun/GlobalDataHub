SELECT
    COUNT(policydeductionid) AS recordcount
FROM
    policydeduction
    INNER JOIN policyline
        ON policyline.policyid = policydeduction.policyid
    INNER JOIN policy
        ON policy.policyid = policydeduction.policyid
        AND policy.outwardpolicyind = 'N'
    INNER JOIN policyprem
        ON policyprem.policyid = policydeduction.policyid
    LEFT JOIN objcode rc
        ON rc.parentid = policydeduction.policyid
        AND rc.codename = 'RiskCode'
        AND rc.parenttable = 'Policy'
    LEFT JOIN objcode tf
        ON tf.parentid = policydeduction.policyid
        AND tf.codename = 'TrustFundCode'
        AND tf.parenttable = 'Policy'