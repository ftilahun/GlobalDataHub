SELECT
    CONCAT(line.line_id, '-', layer_deduction.layer_id, '-', layer_deduction.deduction_id) AS deductionreference,
    CAST(layer_deduction.sequence_no AS STRING) AS deductionsequence,
    line.risk_reference AS policynumber,
    CAST(line.layer_id AS STRING) AS sectionreference,
    CAST(line.line_id AS STRING) AS coveragereference,
    "NDEX" AS sourcesystemcode,
    "NDEX" AS sourcesystemdescription,
    layer.premium_ccy AS currencycode,
    layer_deduction.deduction_code AS deductiontypecode,
    CAST(layer_deduction.deduction_pct AS DECIMAL(18,6)) AS value,
    true AS ispercentage,
    true AS isnetofprevious
FROM
    layer_deduction
    JOIN line
    ON layer_deduction.layer_id = line.layer_id
    JOIN layer
    ON layer_deduction.layer_id = layer.layer_id
WHERE
    layer_deduction.is_contingent <> 'Y'
