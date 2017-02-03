SELECT
    CONCAT(
        "WrittenDeductionsOurShare",
        CAST(policydeduction.policydeductionid AS STRING),
        IF(rc.codevalue IS NOT NULL, rc.codevalue, "MISSING"),
        IF(tf.codevalue IS NOT NULL, tf.codevalue, "MISSING")
        ) AS transactionreference,
    "ECLIPSE" AS sourcesystemcode,
    "ECLIPSE" AS sourcesystemdescription,
    policyline.policylineref AS policynumber,
    CAST(policyline.policylineid AS STRING) AS sectionreference,
    CAST(policyline.policylineid AS STRING) AS coveragereference,
    policy.filcode AS filcode,
    false AS iscashtransactiontype,
    CASE policydeduction.deductionind
        WHEN 'A' THEN policydeduction.origccyiso
        ELSE policyprem.premccyiso
    END AS originalcurrencycode,
    policyprem.premsettccyiso AS settlementcurrencycode,
    CASE policyendorsmnt.effectivedate
        WHEN '' THEN policy.inceptiondate
        WHEN NULL THEN policy.inceptiondate
        ELSE policyendorsmnt.effectivedate
    END AS transactiondate,
    'WrittenDeductionsOurShare' AS transactiontypecode,
    'WrittenDeductionsOurShare' AS transactiontypedescription,
    rc.codevalue AS riskcode,
    CAST(policydeduction.fintranscategoryid AS STRING) AS transactionsubtypecode,
    fintranscategory.categorydsc AS transactionsubtypedescription,
    tf.codevalue AS trustfundcode,
    CAST(
        CASE policydeduction.deductionind
            WHEN 'P' THEN
                (deductiontemptable.net_pct/100.00)
                *
                (
                    ( policydeduction.deductionvalue / 100.00 )
                    *
                    ( policydeduction.applicablepct / 100.00 )
                )
                *
                CAST(COALESCE(policyprem.policypremincome,0) AS DECIMAL(19,2))
                *
                shareofwholepercent.value
                *
                CASE COALESCE( rc.premsplit, CAST(0 AS DECIMAL(10,7)) )
                    WHEN CAST(0 AS DECIMAL(10,2)) THEN CAST(1.00 AS DECIMAL(10,7))
                    ELSE ( rc.premsplit / 100.00 )
                END
                *
                CASE COALESCE( tf.premsplit, CAST(0 AS DECIMAL(10,7)) )
                    WHEN CAST(0 AS DECIMAL(10,2)) THEN CAST(1.00 AS DECIMAL(10,7))
                    ELSE ( tf.premsplit / 100.00 )
                END
            WHEN 'A' THEN
                policydeduction.deductionvalue
                *
                shareofwholepercent.value
            END
        AS DECIMAL(18,6)
    )
    AS originalamount,
    CAST(
        (
            CASE policydeduction.deductionind
                WHEN 'P' THEN
                    (net_pct/100.00)
                    *
                    (
                        ( policydeduction.deductionvalue / 100.00 )
                        *
                        ( policydeduction.applicablepct / 100.00 )
                    )
                    *
                    CAST(COALESCE(policyprem.policypremincome,0) AS DECIMAL(19,2))
                    *
                    shareofwholepercent.value
                    *
                    CASE COALESCE( rc.premsplit, CAST(0 AS DECIMAL(10,7)) )
                        WHEN CAST(0 AS DECIMAL(10,2)) THEN CAST(1.00 AS DECIMAL(10,7))
                        ELSE ( rc.premsplit / 100.00 )
                    END
                    *
                    CASE COALESCE( tf.premsplit, CAST(0 AS DECIMAL(10,7)) )
                        WHEN CAST(0 AS DECIMAL(10,2)) THEN CAST(1.00 AS DECIMAL(10,7))
                        ELSE ( tf.premsplit / 100.00 )
                    END
                WHEN 'A' THEN
                    policydeduction.deductionvalue
                    *
                    shareofwholepercent.value
            END
        )
        /
        policyprem.premccyroe
        *
        policyprem.premsettccyroe
        AS DECIMAL(18,6)
    )
    AS settlementamount

FROM
    policydeduction
    INNER JOIN policyline
        ON policyline.policyid = policydeduction.policyid
    INNER JOIN policy
        ON policy.policyid = policydeduction.policyid
        AND policy.outwardpolicyind = 'N'
    INNER JOIN policyprem
        ON policyprem.policyid = policydeduction.policyid
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
        ON shareofwholepercent.policyid = policydeduction.policyid
    LEFT JOIN policyendorsmnt
        ON policyendorsmnt.policyid = policydeduction.policyid
    LEFT JOIN objcode rc
        ON rc.parentid = policydeduction.policyid
        AND rc.codename = 'RiskCode'
        AND rc.parenttable = 'Policy'
    LEFT JOIN objcode tf
        ON tf.parentid = policydeduction.policyid
        AND tf.codename = 'TrustFundCode'
        AND tf.parenttable = 'Policy'
    LEFT JOIN fintranscategory
        ON fintranscategory.fintranscategoryid = policydeduction.fintranscategoryid
    INNER JOIN (
        SELECT
            pd1.policydeductionid,
            pd1.policyid as policyid,
            net_as_pct_of_gross(pd2.deductionlevel,(pd2.deductionvalue*pd2.applicablepct/100)) as net_pct
        FROM
            policydeduction pd1
            LEFT JOIN policydeduction pd2
                ON pd1.policyid = pd2.policyid
                AND pd2.deductionlevel < pd1.deductionlevel
            GROUP BY pd1.policydeductionid, pd1.policyid
            ) deductiontemptable
        ON deductiontemptable.policydeductionid = policydeduction.policydeductionid