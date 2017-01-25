SELECT
    CONCAT(
        CAST(zucedf00.cemanu AS STRING),".",
        CAST(zucedf00.cemase AS STRING),".",
        CAST(zucedf00.cerkrs AS STRING),".",
        CAST(zucedf00.cesdsq AS STRING),".",
        CAST(zucedf00.cecvsq AS STRING),".",
        CAST(zucedf00.ceedno AS STRING),".",
        CAST(zucedf00.cedgsq AS STRING),".",
        CAST(zudddf00.ddddsq AS STRING)) AS transactionreference,
	"GENIUS" as sourcesystemcode,
	"GENIUS" as sourcesystemdescription,
	CONCAT(zucodf00.comanu, zucodf00.comase, zucodf00.corkrs, zucodf00.cosdsq, zucodf00.cocvsq) AS coveragereference,
    false AS iscashtransactiontype,
	CAST(
	CASE (icdcrep.iavypg)
	    WHEN "PERCNT" THEN
	        ROUND(
                IF(zucedf00.cecva1 IS NOT NULL,zucedf00.cecva1,0) *
                (IF(zuspdf00.spspsp IS NOT NULL,zuspdf00.spspsp, CAST(0 AS DECIMAL(11,7))) / 100.00) *
                IF(zudddf00.ddddpc IS NOT NULL,zudddf00.ddddpc,CAST(0 AS DECIMAL(11,7))) / 100.00, 2)
	    WHEN "FLAT" THEN
            IF(zudddf00.ddddas IS NOT NULL,zudddf00.ddddas,0) *
               IF(SIGN(zucedf00.cecva1) == 0,1,SIGN(zucedf00.cecva1))
        ELSE NULL
	END AS DECIMAL(18,6)) AS originalamount,
    zucedf00.ceogcu AS originalcurrencycode,
	zumadf00.maporf AS policynumber,
	CONCAT(zucodf00.comanu, zucodf00.comase, zucodf00.corkrs, zucodf00.cosdsq) AS sectionreference,
	CAST(
	CASE (icdcrep.iavypg)
	    WHEN "PERCNT" THEN
	        ROUND(
	            CAST(
	            IF((zucedf00.cecva1 / zucedf00.ceeaea) IS NOT NULL,(zucedf00.cecva1 / zucedf00.ceeaea), 0) *
                (IF(zuspdf00.spspsp IS NOT NULL,zuspdf00.spspsp, CAST(0 AS DECIMAL(11,7))) / 100.00) *
                IF(zudddf00.ddddpc IS NOT NULL,zudddf00.ddddpc, CAST(0 AS DECIMAL(11,7))) / 100.00 AS DECIMAL(18,6)), 2)
	    WHEN "FLAT" THEN
	        CAST(
	        IF((zudddf00.ddddas/zucedf00.ceeaea) IS NOT NULL,(zudddf00.ddddas/zucedf00.ceeaea), 0) * IF(SIGN(zucedf00.cecva1) == 0,1,SIGN(zucedf00.cecva1)) AS DECIMAL(18,6))
	    ELSE NULL
	END AS DECIMAL(18,6)) AS settlementamount,
	zucedf00.cecvac AS settlementcurrencycode,
	CAST(CAST(zueldf00.eldtse AS INT) + 19000000 AS STRING) AS transactiondate,
	"WrittenDeductionsOurShare" AS transactiontypecode,
    "WrittenDeductionsOurShare" AS transactiontypedescription,
	SUBSTRING(icdcrep.iaddcd,1,3) AS transactionsubtypecode,
    icdcrep.iaddds AS transactionsubtypedescription,
	CAST(NULL AS STRING) AS filcode,
	CAST(NULL AS STRING) AS riskcode,
	CAST(NULL AS STRING) AS trustfundcode
FROM zucedf00
INNER JOIN zueldf00 ON zucedf00.cemanu = zueldf00.elmanu
	AND zucedf00.cemase = zueldf00.elmase
	AND zucedf00.ceedno = zueldf00.eledno
INNER JOIN zucodf00 ON zucedf00.cemanu = zucodf00.comanu
	AND zucedf00.cemase = zucodf00.comase
	AND zucedf00.cerkrs = zucodf00.corkrs
	AND zucedf00.cesdsq = zucodf00.cosdsq
	AND zucedf00.cecvsq = zucodf00.cocvsq
INNER JOIN zumadf00 ON zucedf00.cemanu = zumadf00.mamanu
	AND zucedf00.cemase = zumadf00.mamase
	AND zumadf00.maporf NOT LIKE 'X%' and zumadf00.maporf NOT LIKE '9%'
INNER JOIN zudgdf00 ON zucedf00.cemanu = zudgdf00.dgmanu
	AND zucedf00.cemase = zudgdf00.dgmase
	AND zucedf00.cedgsq = zudgdf00.dgdgsq
INNER JOIN zudvdf00 ON zudgdf00.dgmanu = zudvdf00.dvmanu
	AND zudgdf00.dgmase = zudvdf00.dvmase
	AND zudgdf00.dgdgsq = zudvdf00.dvdgsq
INNER JOIN zudddf00 ON zudvdf00.dvmanu = zudddf00.ddmanu
	AND zudvdf00.dvmase = zudddf00.ddmase
	AND zudvdf00.dvdgsq = zudddf00.dddgsq
	AND zudvdf00.dvdgvs = zudddf00.dddgvs
INNER JOIN icdcrep ON zudddf00.ddddcd = icdcrep.iaddcd
LEFT JOIN zusfdf00 ON zusfdf00.sfmanu = zucedf00.cemanu
	AND zusfdf00.sfmase = zucedf00.cemase
	AND zusfdf00.sfrkrs = zucedf00.cerkrs
	AND zusfdf00.sfsdsq = zucedf00.cesdsq
LEFT JOIN zuskdf00 ON zuskdf00.skmanu = zusfdf00.sfmanu
	AND zuskdf00.skmase = zusfdf00.sfmase
	AND zuskdf00.skrkrs = zusfdf00.sfrkrs
LEFT JOIN zugsdf00 ON zucedf00.cemanu = zugsdf00.gsmanu
	AND zucedf00.cemase = zugsdf00.gsmase
	AND zugsdf00.gsgsgn = zuskdf00.skgsgn
LEFT JOIN zugpdf00 ON zugsdf00.gsgsgn = zugpdf00.gpgsgn
	AND zugpdf00.gpgpms = 1
LEFT JOIN zuspdf00 ON zugsdf00.gsgsgn = zuspdf00.spgsgn
	AND zugpdf00.gpgpmt = zuspdf00.spgpmt
	AND zugpdf00.gpgpsq = zuspdf00.spgpsq
	AND zuspdf00.spspsq = '01'