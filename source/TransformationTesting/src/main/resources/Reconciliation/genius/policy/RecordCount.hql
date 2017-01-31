SELECT
    COUNT(maporf)
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
WHERE maporf NOT LIKE 'X%'
AND maporf NOT LIKE '9%'