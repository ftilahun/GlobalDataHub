SELECT
    COUNT(policyline.policyid) AS recordcount
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