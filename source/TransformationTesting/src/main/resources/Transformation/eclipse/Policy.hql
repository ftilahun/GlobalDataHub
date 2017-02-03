SELECT

    policyline.policylineref AS policynumber,
    CAST(policyline.policylineid AS STRING) AS coveragereference,
    CAST(policyline.policylineid AS STRING) AS sectionreference,
    "ECLIPSE" AS sourcesystemcode,
    "ECLIPSE" AS sourcesystemdescription,
    policyline.producingteam AS branchcode,
    branchbusinesscode.dsc AS branchdescription,
    policy.canceldate AS cancellationdate,
    policyendorsmnt.effectivedate AS coverageeffectivefromdate,
    policy.expirydate AS coverageeffectivetodate,
    policy.expirydate AS expirydate,
    policy.filcode AS filcode,
    policy.inceptiondate AS inceptiondate,
    addr.domicilecountry AS insureddomicilecode,
    CAST(NULL AS STRING) AS legacypolicynumber,
    policyline.synd AS legalentitycode,
    class4.codevalue AS lineofbusinesscode,
    lobbusinesscode.dsc AS lineofbusinessdescription,
    childrenpolicy.policylineref AS linkedmasterreference,
    rc.codevalue AS majorriskcode,
    tf.codevalue AS majortrustfundcode,
    policy.placingtype AS methodofplacementcode,
    mopbusinesscode.dsc AS methodofplacementdescription,
    CAST(NULL AS STRING) AS risklocationcode,
    policyline.policylineref AS sourcesystempolicynumber,
    class1.codevalue AS sublineofbusinesscode,
    sublobbusinesscode.dsc AS sublineofbusinessdescription,
    policy.uniquemarketref AS uniquemarketreference,
    CAST(policy.yoa AS INT) AS yearofaccount,
    policyline.linestatus AS policystatuscode,
    CAST(policyline.estsigningdown AS DECIMAL(12,7)) AS estimatedsignedpercent,
    CAST(policyline.signedorder AS DECIMAL(12,7)) AS signedorderpercent,
    CAST(policyline.signedline AS DECIMAL(12,7)) AS signedlinepercent,
    CAST(policyline.writtenline AS DECIMAL(12,7)) AS writtenlinepercent,
    CAST(policyline.writtenorder AS DECIMAL(12,7)) AS writtenorderpercent,
    IF(policyline.linestatus = 'Signed',
        CASE policyline.wholepartorder
            WHEN 'O' THEN CAST(policyline.signedline / 100 * policyline.signedorder / 100 AS DECIMAL(12,7))
            WHEN 'W' THEN CAST(policyline.signedline / 100  AS DECIMAL(12,7))
        END,
        CASE policyline.wholepartorder
            WHEN 'O' THEN CAST(policyline.writtenline / 100 * policyline.writtenorder / 100 * policyline.estsigningdown / 100 AS DECIMAL(12,7))
            WHEN 'W' THEN CAST(policyline.writtenline / 100 * policyline.estsigningdown / 100 AS DECIMAL(12,7))
        END
    ) AS shareofwholepercent

FROM

    policyline
    INNER JOIN policy
    ON policy.policyid = policyline.policyid AND policy.outwardpolicyind = 'N'
    INNER JOIN policyorg
    ON policyline.policyid = policyorg.policyid
    INNER JOIN role
    ON role.orgid = policyorg.orgid AND role.role = 'ASSURED'
    INNER JOIN org
    ON org.orgid = role.orgid
    LEFT JOIN policyendorsmnt
    ON policyline.policyid = policyendorsmnt.policyid
    LEFT JOIN addr
    ON org.orgid = addr.orgid AND addr.addrtype = 'Head Office'
    LEFT JOIN objcode class1
    ON class1.parentid = policyline.policyid AND class1.parenttable = 'Policy' AND class1.codename = 'Class1'
    LEFT JOIN objcode rc
    ON rc.parentid = policyline.policyid AND rc.parenttable = 'Policy' AND rc.codename = 'RiskCode'
    LEFT JOIN objcode tf
    ON tf.parentid = policyline.policyid AND tf.parenttable = 'Policy' AND tf.codename = 'TrustFundCode'
    LEFT JOIN objcode class4
    ON class4.parentid = policyline.policyid AND class4.parenttable = 'Policy' AND class4.codename = 'Class4'
    LEFT JOIN (
        SELECT childbc.value, childbc.dsc
        FROM businesscode parentbc
        INNER JOIN businesscode childbc
        ON parentbc.businesscodeid = childbc.parentcodeid AND parentbc.value = 'ProducingTeam'
    ) AS branchbusinesscode
    ON branchbusinesscode.value = policyline.producingteam
    LEFT JOIN (
        SELECT childbc.value, childbc.dsc
        FROM businesscode parentbc
        INNER JOIN businesscode childbc
        ON parentbc.businesscodeid = childbc.parentcodeid AND parentbc.value = 'ClassType'
    ) AS lobbusinesscode
    ON lobbusinesscode.value = class4.codevalue
    LEFT JOIN (
        SELECT childbc.value, childbc.dsc
        FROM businesscode parentbc
        INNER JOIN businesscode childbc
        ON parentbc.businesscodeid = childbc.parentcodeid AND parentbc.value = 'MajorClass'
    ) AS sublobbusinesscode
    ON sublobbusinesscode.value = class1.codevalue
    LEFT JOIN (
        SELECT childbc.value, childbc.dsc
        FROM businesscode parentbc
        INNER JOIN businesscode childbc
        ON parentbc.businesscodeid = childbc.parentcodeid AND parentbc.value = 'PlacingBasis'
    ) AS mopbusinesscode
    ON mopbusinesscode.value = policy.placingtype
    LEFT JOIN (
     SELECT childpolicy.policyid, childpolicy.parentpolicyid, parentpolicy.policylineref
     FROM policy childpolicy
     INNER JOIN policyline parentpolicy
     ON childpolicy.parentpolicyid = parentpolicy.policyid
    ) AS childrenpolicy
    ON policy.policyid = childrenpolicy.policyid
