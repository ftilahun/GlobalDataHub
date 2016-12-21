SELECT
    CONCAT(line.line_id, '-', layer_deduction.layer_id, '-', layer_deduction.deduction_id) AS deductionreference,
    CAST(layer_deduction.sequence_no AS STRING) AS deductionsequence,
    line.risk_reference AS policynumber,
    CAST(line.layer_id AS STRING) AS sectionreference,
    CAST(line.line_id AS STRING) AS coveragereference,
    "NDEX" AS sourcesystemcode,
    layer.premium_ccy AS currencycode,
    layer_deduction.deduction_code AS deductiontypecode,
    CAST(
        CASE line.business_type
            WHEN '1' THEN '0'
            WHEN '17' THEN '0'
            ELSE
                line.slip_income_amount * (line.reporting_line_pct/100)
                * ((tmptable.net_pct/100) * (layer_deduction.deduction_pct/100))
        END
        AS DECIMAL(18,6)) AS calculateddeductionamount,
    CAST(layer_deduction.deduction_pct AS DECIMAL(18,6)) AS value,
    true AS ispercentage,
    true AS isnetofprevious
FROM
    layer_deduction
    JOIN line
    ON layer_deduction.layer_id = line.layer_id
    JOIN layer
    ON layer_deduction.layer_id = layer.layer_id
    JOIN (
        SELECT
            ld1.deduction_id,
            ld1.layer_id AS layer_id,
            CAST(net_as_pct_of_gross(ld2.sequence_no,CAST(ld2.deduction_pct AS DECIMAL(10,2))) AS STRING) AS net_pct
        FROM layer_deduction ld1
        LEFT JOIN layer_deduction ld2
            ON ld1.layer_id = ld2.layer_id
            AND ld2.sequence_no < ld1.sequence_no
        GROUP BY ld1.deduction_id, ld1.layer_id
    ) AS tmptable
    ON tmptable.deduction_id = layer_deduction.deduction_id