SELECT
    CONCAT("WrittenPremiumShare", CAST(policyprem.policypremid AS STRING)) AS transactionreference,
    "ECLIPSE" AS sourcesystemcode,
    "ECLIPSE" AS sourcesystemdescription,
    CAST(policyline.policylineid AS STRING) AS coveragereference,
    false AS iscashtransactiontype,
    CAST(policyprem.policypremincome * objcode.premsplit(riskcode) * objcode.premsplit(trustfundcode) * shareofwholepercent AS DECIMAL(18:6)) AS originalamount,
    CAST(policyprem.premccyiso AS STRING) AS originalcurrencycode,
    CAST(policyline.policylineref AS STRING) AS policynumber,
    CAST(policyline.policylineid AS STRING) AS sectionreference,
    CAST(policyprem.policypremincome * objcode.premsplit (RiskCode) * objcode.premsplit (TrustFundCode) / policyprem.premccyroe * policyprem.premsettsccyroe * ShareOfWholePercent(in ECM) AS DECIMAL(18:6)) AS settlementamount,
    CAST(policyprem.premsettccyiso AS STRING) AS settlementcurrencycode,
    CAST(IF(policyendorsmnt.effectivedate IS NOT NULL, policyendorsmnt.effectivedate, policy.inceptiondate) AS STRING) AS transactiondate,
    "WrittenPremiumShare" AS transactiontypecode,
    "WrittenPremiumShare" AS transactiontypedescription,
    CAST(NULL AS STRING) AS transactionsubtypecode,
    CAST(NULL AS STRING) AS transactionsubtypedescription,
    CAST(policy.filcode AS STRING) AS filcode,
    CAST(objcode.codevalue AS STRING) AS riskcode,
    CAST(objcode.codevalue AS STRING) AS trustfundcode
FROM
    policyprem
    INNER JOIN policyline ON policyprem.policyid = policyline.policyid
    INNER JOIN policy ON policyline.policyid = policy.policyid
    LEFT JOIN objcode ON objcode.parentid = policyline.policyid AND objcode.codename = 'RiskCode' AND objcode.parenttable = 'Policy'
    LEFT JOIN policyendorsmnt ON policyline.policyid = policyendorsmnt.policyid