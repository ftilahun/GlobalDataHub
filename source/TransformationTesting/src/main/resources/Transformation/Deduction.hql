SELECT
    CAST(layer_deduction.deduction_id AS STRING) AS deductionreference,
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
            ELSE line.slip_income_amount * (line.reporting_line_pct/100) * (layer_deduction.deduction_pct/100)
        END
        AS DECIMAL(12,7)) AS calculateddeductionamount,
    layer_deduction.deduction_pct AS value,
    true AS ispercentage,
    true AS isnetofprevious
FROM
    layer_deduction
    JOIN line
    ON layer_deduction.layer_id = line.layer_id
    JOIN layer
    ON layer_deduction.layer_id = layer.layer_id