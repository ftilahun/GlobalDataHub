SELECT
    underwriting_block.block AS lineofbusinesscode,
    underwriting_block.subblock AS sublineofbusinesscode,
    "NDEX" AS sourcesystemcode,
    lookup_block.description AS lineofbusinessdescription,
    underwriting_block.description AS sublineofbusinessdescription
FROM
    lookup_block JOIN underwriting_block
ON
    (lookup_block.block = underwriting_block.block)