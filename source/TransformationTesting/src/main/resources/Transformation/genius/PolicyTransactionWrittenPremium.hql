SELECT

    CONCAT(zucedf00.cemanu, ".", zucedf00.cemase, ".", zucedf00.cerkrs, ".",
        zucedf00.cesdsq, ".", zucedf00.cecvsq, ".", zucedf00.ceedno) AS transactionreference,
    "GENIUS" AS sourcesystemcode,
    "GENIUS" AS sourcesystemdescription,
    CONCAT(zucodf00.comanu, zucodf00.comase, zucodf00.corkrs, zucodf00.cosdsq, zucodf00.cocvsq) AS coveragereference,
    false AS iscashtransactiontype,
    CAST(ROUND(
        IF(zucedf00.cecva1 IS NOT NULL, zucedf00.cecva1, CAST(0 AS DECIMAL(15,2))) *
        IF(zuspdf00.spspsp IS NOT NULL, zuspdf00.spspsp, CAST(0 AS DECIMAL(11,7)))
        / 100.0, 2 ) AS DECIMAL(18,6)) AS originalamount,
    zucedf00.ceogcu AS originalcurrencycode,
    zumadf00.maporf AS policynumber,
    CONCAT(zucodf00.comanu, zucodf00.comase, zucodf00.corkrs, zucodf00.cosdsq) AS sectionreference,
    CAST(ROUND(
        IF((zucedf00.cecva1 / zucedf00.ceeaea) IS NOT NULL, zucedf00.cecva1 / zucedf00.ceeaea, 0) *
        IF(zuspdf00.spspsp IS NOT NULL, zuspdf00.spspsp, CAST(0 AS DECIMAL(11,7)))
        / 100.0, 2 ) AS DECIMAL(18,6)) AS settlementamount,
    zucedf00.cecvac AS settlementcurrencycode,
    CAST(CAST(zueldf00.eldtse AS INT) + 19000000 AS STRING) AS transactiondate,
    "WrittenPremiumOurShare" AS transactiontypecode,
    "WrittenPremiumOurShare" AS transactiontypedescription,
    "PRM" AS transactionsubtypecode,
    "Premium" AS transactionsubtypedescription,
    CAST(NULL AS STRING) AS filcode,
    CAST(NULL AS STRING) AS riskcode,
    CAST(NULL AS STRING) AS trustfundcode

FROM

    zucedf00
    INNER JOIN zueldf00
    ON zucedf00.cemanu = zueldf00.elmanu AND zucedf00.cemase = zueldf00.elmase AND zucedf00.ceedno = zueldf00.eledno
    INNER JOIN zucodf00
    ON zucedf00.cemanu = zucodf00.comanu AND zucedf00.cemase = zucodf00.comase AND zucedf00.cerkrs = zucodf00.corkrs
    AND zucedf00.cesdsq = zucodf00.cosdsq AND zucedf00.cecvsq = zucodf00.cocvsq
    INNER JOIN zumadf00
    ON zucedf00.cemanu = zumadf00.mamanu AND zucedf00.cemase = zumadf00.mamase
    LEFT JOIN zusfdf00
    ON zusfdf00.sfmanu = zucedf00.cemanu AND zusfdf00.sfmase = zucedf00.cemase AND zusfdf00.sfrkrs = zucedf00.cerkrs
    AND zusfdf00.sfsdsq = zucedf00.cesdsq
    LEFT JOIN zuskdf00
    ON zuskdf00.skmanu = zusfdf00.sfmanu AND zuskdf00.skmase = zusfdf00.sfmase AND zuskdf00.skrkrs = zusfdf00.sfrkrs
    LEFT JOIN zugsdf00
    ON zumadf00.mamanu = zugsdf00.gsmanu AND zumadf00.mamase = zugsdf00.gsmase AND zugsdf00.gsgsgn = zuskdf00.skgsgn
    LEFT JOIN zugpdf00
    ON zugsdf00.gsgsgn = zugpdf00.gpgsgn AND zugpdf00.gpgpms = 1
    LEFT JOIN zuspdf00
    ON zugsdf00.gsgsgn = zuspdf00.spgsgn AND gpgpmt = zuspdf00.spgpmt AND zugpdf00.gpgpsq = zuspdf00.spgpsq
    AND zuspdf00.spspsq = '01' AND maporf NOT LIKE 'X%' AND maporf NOT LIKE '9%'
