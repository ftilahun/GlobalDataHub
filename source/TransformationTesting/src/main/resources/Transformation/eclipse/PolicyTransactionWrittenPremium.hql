SELECT
    CONCAT("WrittenPremiumShare", CAST(policyprem.policypremid AS STRING)) AS transactionreference,
    "ECLIPSE" AS sourcesystemcode,
    "ECLIPSE" AS sourcesystemdescription,
    policyline.policylineid AS coveragereference,
    false AS iscashtransactiontype,
    AS originalamount,
    policyprem.premccyiso AS originalcurrencycode,
    policyline.policylineref AS policynumber,
    policyline.policylineid AS sectionreference,
    AS settlementamount,
    policyprem.premsettccyiso AS settlementcurrencycode,
    AS transactiondate,
    "WrittenPremiumShare" AS transactiontypecode,
    "WrittenPremiumShare" AS transactiontypedescription,
    CAST(NULL AS STRING) AS transactionsubtypecode,
    CAST(NULL AS STRING) AS transactionsubtypedescription,
    policy.filcode AS filcode,
    objcode.codevalue AS riskcode,
    not in the doc - need to check AS trustfundcode
FROM
    policyprem
    INNER JOIN policyline ON policyprem.policyid = policyline.policyid
    INNER JOIN policy ON policyline.policyid = policy.policyid
    LEFT JOIN objcode ON objcode.parentid = policyline.policyid AND objcode.codename = 'RiskCode' AND objcode.parenttable = 'Policy'
    LEFT JOIN policyendorsmnt ON policyline.policyid = policyendorsmnt.policyid