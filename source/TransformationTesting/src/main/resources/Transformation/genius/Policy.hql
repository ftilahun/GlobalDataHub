SELECT

    maporf AS policynumber,
    CONCAT(comanu, comase, corkrs, cosdsq, cocvsq) AS coveragereference,
    CONCAT(comanu, comase, corkrs, cosdsq) AS sectionreference,
    "GENIUS" AS sourcesystemcode,
    "GENIUS" AS sourcesystemdescription,
    mamabn AS branchcode,
    nananm AS branchdescription,
    CAST(CAST(maekdt AS INT) + 19000000 AS STRING) AS cancellationdate,
    CAST(CAST(cocpst AS INT) + 19000000 AS STRING) AS coverageeffectivefromdate,
    CAST(CAST(cocped AS INT) + 19000000 AS STRING) AS coverageeffectivetodate,
    CAST(CAST(maekdt AS INT) + 19000000 AS STRING) AS expirydate,
    CAST(NULL AS STRING) AS filcode,
    CAST(CAST(maogic AS INT) + 19000000 AS STRING) AS inceptiondate,
    COALESCE(
        CONCAT("[",zipcode,"][",city,"][",coalesce(us_statecode,statecode),"][",countrycode,"]"),
        "[MISSING][MISSING][MISSING][MISSING]"
         ) AS insureddomicilecode,
    maporf AS legacypolicynumber,
    mamabn AS legalentitycode,
    madpcd AS lineofbusinesscode,
    dpdpds AS lineofbusinessdescription,
    CONCAT(rimanu,rimase) AS linkedmasterreference,
    CAST(NULL AS STRING) AS majorriskcode,
    CAST(NULL AS STRING) AS majortrustfundcode,
    LTRIM(RTRIM(myco04)) AS methodofplacementcode,
    cvv235 AS methodofplacementdescription,
    COALESCE(
        CONCAT("[",zipcode,"][",city,"][",coalesce(us_statecode,statecode),"][",countrycode,"]"),
        "[MISSING][MISSING][MISSING][MISSING]"
         ) AS risklocationcode,
    maporf AS sourcesystempolicynumber,
    mamapc AS sublineofbusinesscode,
    kpksln AS sublineofbusinessdescription,
    mamati AS uniquemarketreference,
    CAST(CAST(mauwyr AS INT) + 1900 AS INT) AS yearofaccount,
    masscd AS policystatuscode,
    CAST(spspes AS DECIMAL(12,7)) AS estimatedsignedpercent,
    CAST(spsats AS DECIMAL(12,7)) AS signedorderpercent,
    CAST(spspsl AS DECIMAL(12,7)) AS signedlinepercent,
    CAST(spspwl AS DECIMAL(12,7)) AS writtenlinepercent,
    CAST(sppcop AS DECIMAL(12,7)) AS writtenorderpercent,
    CAST(spspsp AS DECIMAL(12,7)) AS shareofwholepercent

FROM zumadf00

    INNER JOIN zuskdf00
        ON zumadf00.mamanu = zuskdf00.skmanu
        AND zumadf00.mamase = zuskdf00.skmase
    INNER JOIN zusfdf00
        ON zuskdf00.skmanu = zusfdf00.sfmanu
        AND zuskdf00.skmase = zusfdf00.sfmase
        AND zuskdf00.skrkrs = zusfdf00.sfrkrs
    INNER JOIN zucodf00
        ON zusfdf00.sfmanu = zucodf00.comanu
        AND zusfdf00.sfmase = zucodf00.comase
        AND zuskdf00.skrkrs = zucodf00.corkrs
        AND zusfdf00.sfsdsq = zucodf00.cosdsq
    LEFT JOIN znnadf00
        ON zumadf00.mamabn = znnadf00.nanacd
    LEFT JOIN zhdpdf00
        ON zumadf00.madpcd = dpdpcd
    LEFT JOIN zuridf00
        ON zuridf00.ririmn = zumadf00.mamanu
        AND zuridf00.ririms = zumadf00.mamase
    LEFT JOIN zpkpdf00
        ON zumadf00.mamapc = zpkpdf00.kpprco
    LEFT JOIN zum0df00
        ON zucodf00.comanu = zum0df00.m0manu
        AND zucodf00.comase = zum0df00.m0mase
        AND zucodf00.corkrs = zum0df00.m0rkrs
        AND zucodf00.cosdsq = zum0df00.m0sdsq
        AND zucodf00.cocvsq = zum0df00.m0cvsq
    LEFT JOIN zumydf00
        ON zum0df00.m0edsg = zumydf00.myedsg
    LEFT JOIN zhcvdf00
        ON TRIM(zumydf00.myco04) = TRIM(zhcvdf00.cvv243)
        AND MYEDDC = 'MPPR'
        AND CVVG01 = 'MP'
    LEFT JOIN zugsdf00
        ON zugsdf00.gsmanu = zuskdf00.skmanu
        AND zugsdf00.gsmase = zuskdf00.skmase
        AND zugsdf00.gsgsgn = zuskdf00.skgsgn
    LEFT JOIN zugpdf00
        ON zugpdf00.gpgsgn = zugsdf00.gsgsgn
        AND zugpdf00.gpgpmt = zugsdf00.gsgpmt
        AND zugpdf00.gpgpms = '1'
    LEFT JOIN zuspdf00
        ON zuspdf00.spgsgn = zugpdf00.gpgsgn
        AND zuspdf00.spgpmt = zugpdf00.gpgpmt
        AND zuspdf00.spgpsq = zugpdf00.gpgpsq
        AND zuspdf00.spspsq = '01'
    LEFT JOIN (
        SELECT DISTINCT
            IF( TRIM(dxqiaz) IN ('','-','.'), "MISSING", TRIM(dxqiaz) ) AS zipcode,
            IF( TRIM(dxnae5) IN ('','-','.'), "MISSING", TRIM(dxnae5) ) AS city,
            IF( TRIM(aaaacd) LIKE 'US__', TRIM(aaaak1), NULL) AS us_statecode,
            IF( TRIM(dxa2cd) IN ('','-','.'), "MISSING", TRIM(dxa2cd) ) AS statecode,
            IF( TRIM(aaaacd) LIKE 'US__', TRIM(aaaasd), NULL) AS us_statedescription,
            IF( TRIM(dxnae7) IN ('','-','.'), "MISSING", TRIM(dxnae7) ) AS statedescription,
            IF( TRIM(dxa1cd) IN ('','-','.'), "MISSING", TRIM(dxa1cd) ) AS countrycode,
            dxnacd,
            aaaacd
        FROM zndxdf00
        INNER JOIN znnzdf00
            ON znnzdf00.nznacd = zndxdf00.dxnacd
            AND znnzdf00.nzadcd = zndxdf00.dxadcd
            AND znnzdf00.nzadfg = '1'
        INNER JOIN zhaadf00
            ON TRIM(zhaadf00.aaaacd) = TRIM(zndxdf00.dxa1cd)
        ) geography
        ON geography.dxnacd = zumadf00.mapnsn
    WHERE maporf NOT LIKE 'X%'
    AND maporf NOT LIKE '9%'