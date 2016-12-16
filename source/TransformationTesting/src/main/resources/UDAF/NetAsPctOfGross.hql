SELECT
    in1.id,
    in1.grouping_id AS grouping_id,
    CAST(net_as_pct_of_gross(in2.sequence_no,CAST(in2.gross_pct AS DECIMAL(10,2))) AS STRING) AS net_pct
FROM input in1
LEFT JOIN input in2
    ON in1.grouping_id = in2.grouping_id
    AND in2.sequence_no < in1.sequence_no
GROUP BY in1.id, in1.grouping_id