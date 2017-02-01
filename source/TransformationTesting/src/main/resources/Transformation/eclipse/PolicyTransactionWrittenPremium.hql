SELECT
    CONCAT("WrittenPremiumShare", CAST(policyprem.policypremid AS STRING)) AS transactionreference,
    "ECLIPSE" AS sourcesystemcode,
    "ECLIPSE" AS sourcesystemdescription,
    CAST(policyline.policylineid AS STRING) AS coveragereference,
    false AS iscashtransactiontype,
    CAST(policyprem.policypremincome *
         CASE COALESCE( objcode1.premsplit, CAST(0 AS DECIMAL(10,7)) )
                    WHEN CAST(0 AS DECIMAL(10,7)) THEN CAST(1.00 AS DECIMAL(10,7))
                    ELSE ( objcode1.premsplit / 100.00 )
                END
                *
         CASE COALESCE( objcode2.premsplit, CAST(0 AS DECIMAL(10,7)) )
                    WHEN CAST(0 AS DECIMAL(10,7)) THEN CAST(1.00 AS DECIMAL(10,7))
                    ELSE ( objcode2.premsplit / 100.00 )
                END
                *
         (IF(policyline.linestatus = 'Signed',
                CASE policyline.wholepartorder
                    WHEN 'O' THEN CAST(policyline.signedline / 100 * policyline.signedorder / 100 AS DECIMAL(12,7))
                    WHEN 'W' THEN CAST(policyline.signedline / 100  AS DECIMAL(12,7))
                END,
                CASE policyline.wholepartorder
                    WHEN 'O' THEN CAST(policyline.writtenline / 100 * policyline.writtenorder / 100 * policyline.estsigningdown / 100 AS DECIMAL(12,7))
                    WHEN 'W' THEN CAST(policyline.writtenline / 100 * policyline.estsigningdown / 100 AS DECIMAL(12,7))
                END
            ))
    AS DECIMAL(18,6)) AS originalamount,
    CAST(policyprem.premccyiso AS STRING) AS originalcurrencycode,
    CAST(policyline.policylineref AS STRING) AS policynumber,
    CAST(policyline.policylineid AS STRING) AS sectionreference,
    CAST(policyprem.policypremincome * --Makes null
         CASE COALESCE( objcode1.premsplit, CAST(0 AS DECIMAL(10,7)) )
                    WHEN CAST(0 AS DECIMAL(10,7)) THEN CAST(1.00 AS DECIMAL(10,7))
                    ELSE ( objcode1.premsplit / 100.00 )
                END
                *
         CASE COALESCE( objcode2.premsplit, CAST(0 AS DECIMAL(10,7)) )
                    WHEN CAST(0 AS DECIMAL(10,7)) THEN CAST(1.00 AS DECIMAL(10,7))
                    ELSE ( objcode2.premsplit / 100.00 )
                END
                /
         policyprem.premccyroe *    --- Works
         policyprem.premsettccyroe * -- works
        (IF(policyline.linestatus = 'Signed',
                CASE policyline.wholepartorder
                     WHEN 'O' THEN CAST(policyline.signedline / 100 * policyline.signedorder / 100 AS DECIMAL(12,7))
                     WHEN 'W' THEN CAST(policyline.signedline / 100  AS DECIMAL(12,7))
                END,
                CASE policyline.wholepartorder
                     WHEN 'O' THEN CAST(policyline.writtenline / 100 * policyline.writtenorder / 100 * policyline.estsigningdown / 100 AS DECIMAL(12,7))
                     WHEN 'W' THEN CAST(policyline.writtenline / 100 * policyline.estsigningdown / 100 AS DECIMAL(12,7))
                END
            ))
    AS DECIMAL(18,6)) AS settlementamount,
    CAST(policyprem.premsettccyiso AS STRING) AS settlementcurrencycode,
    CAST(IF(policyendorsmnt.effectivedate IS NOT NULL, policyendorsmnt.effectivedate, policy.inceptiondate) AS STRING) AS transactiondate,
    "WrittenPremiumShare" AS transactiontypecode,
    "WrittenPremiumShare" AS transactiontypedescription,
    CAST(NULL AS STRING) AS transactionsubtypecode,
    CAST(NULL AS STRING) AS transactionsubtypedescription,
    CAST(policy.filcode AS STRING) AS filcode,
    CAST(objcode1.codevalue AS STRING) AS riskcode,
    CAST(objcode2.codevalue AS STRING) AS trustfundcode
FROM
    policyprem
    INNER JOIN policyline ON policyprem.policyid = policyline.policyid
    INNER JOIN policy ON policyline.policyid = policy.policyid
    LEFT JOIN objcode objcode1 ON objcode1.parentid = policyline.policyid
    AND objcode1.codename = 'RiskCode' AND objcode1.parenttable = 'Policy'
    LEFT JOIN objcode objcode2 ON objcode2.parentid = policyline.policyid
    AND objcode2.codename = 'TrustFundCode' AND objcode2.parenttable = 'Policy'
    LEFT JOIN policyendorsmnt ON policyline.policyid = policyendorsmnt.policyid