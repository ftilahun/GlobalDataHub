SELECT COUNT(*) AS lineofbusiness_count
FROM
    lookup_block JOIN underwriting_block
ON
    (lookup_block.block = underwriting_block.block)