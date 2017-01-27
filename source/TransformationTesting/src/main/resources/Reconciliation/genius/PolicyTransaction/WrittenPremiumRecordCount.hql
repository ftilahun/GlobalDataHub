SELECT

    COUNT(zumadf00.maporf) AS recordcount

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