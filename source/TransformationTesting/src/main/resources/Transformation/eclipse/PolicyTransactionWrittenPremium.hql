SELECT
    CONCAT(
        CAST(policyprem.policyid AS STRING),
        "WrittenPremiumOurShare",
        CAST(policyprem.policypremid AS STRING),
        IF(rc.codevalue IS NOT NULL, rc.codevalue, "MISSING"),
        IF(tf.codevalue IS NOT NULL, tf.codevalue, "MISSING")) AS transactionreference,
    "ECLIPSE" AS sourcesystemcode,
    "ECLIPSE" AS sourcesystemdescription,
    CAST(policyline.policylineid AS STRING) AS coveragereference,
    false AS iscashtransactiontype,
    CAST(policyprem.policypremincome *
         CASE COALESCE( rc.premsplit, CAST(0 AS DECIMAL(10,7)) )
            WHEN CAST(0 AS DECIMAL(10,7)) THEN CAST(1.00 AS DECIMAL(10,7))
            ELSE ( rc.premsplit / 100.00 )
         END
         *
         CASE COALESCE( tf.premsplit, CAST(0 AS DECIMAL(10,7)) )
            WHEN CAST(0 AS DECIMAL(10,7)) THEN CAST(1.00 AS DECIMAL(10,7))
            ELSE ( tf.premsplit / 100.00 )
         END
         *
         shareofwholepercent.value
    AS DECIMAL(18,6)) AS originalamount,
    CAST(policyprem.premccyiso AS STRING) AS originalcurrencycode,
    CAST(policyline.policylineref AS STRING) AS policynumber,
    CAST(policyline.policylineid AS STRING) AS sectionreference,
    CAST(policyprem.policypremincome *
         CASE COALESCE( rc.premsplit, CAST(0 AS DECIMAL(10,7)) )
            WHEN CAST(0 AS DECIMAL(10,7)) THEN CAST(1.00 AS DECIMAL(10,7))
            ELSE ( rc.premsplit / 100.00 )
         END
         *
         CASE COALESCE( tf.premsplit, CAST(0 AS DECIMAL(10,7)) )
            WHEN CAST(0 AS DECIMAL(10,7)) THEN CAST(1.00 AS DECIMAL(10,7))
            ELSE ( tf.premsplit / 100.00 )
         END
         /
         policyprem.premccyroe *
         policyprem.premsettccyroe *
         shareofwholepercent.value
    AS DECIMAL(18,6)) AS settlementamount,
    policyprem.premsettccyiso AS settlementcurrencycode,
    CAST(IF(policyendorsmnt.effectivedate IS NOT NULL, policyendorsmnt.effectivedate, policy.inceptiondate) AS STRING) AS transactiondate,
    "WrittenPremiumOurShare" AS transactiontypecode,
    "WrittenPremiumOurShare" AS transactiontypedescription,
    CAST(NULL AS STRING) AS transactionsubtypecode,
    CAST(NULL AS STRING) AS transactionsubtypedescription,
    CAST(policy.filcode AS STRING) AS filcode,
    CAST(rc.codevalue AS STRING) AS riskcode,
    CAST(tf.codevalue AS STRING) AS trustfundcode
FROM
    policyprem
    INNER JOIN policyline
    ON policyprem.policyid = policyline.policyid
    INNER JOIN policy
    ON policyline.policyid = policy.policyid
    LEFT JOIN (
        SELECT
            policyline.policyid,
            IF(policyline.linestatus = 'Signed',
                CASE policyline.wholepartorder
                    WHEN 'O' THEN CAST(policyline.signedline / 100 * policyline.signedorder / 100 AS DECIMAL(12,7))
                    WHEN 'W' THEN CAST(policyline.signedline / 100  AS DECIMAL(12,7))
                END,
                CASE policyline.wholepartorder
                    WHEN 'O' THEN CAST(policyline.writtenline / 100 * policyline.writtenorder / 100 * policyline.estsigningdown / 100 AS DECIMAL(12,7))
                    WHEN 'W' THEN CAST(policyline.writtenline / 100 * policyline.estsigningdown / 100 AS DECIMAL(12,7))
                END
            ) AS value
            FROM policyline
        ) shareofwholepercent
    ON shareofwholepercent.policyid = policyprem.policyid
    LEFT JOIN objcode rc
    ON rc.parentid = policyline.policyid
    AND rc.codename = 'RiskCode' AND rc.parenttable = 'Policy'
    LEFT JOIN objcode tf
    ON tf.parentid = policyline.policyid
    AND tf.codename = 'TrustFundCode' AND tf.parenttable = 'Policy'
    LEFT JOIN policyendorsmnt
    ON policyline.policyid = policyendorsmnt.policyid