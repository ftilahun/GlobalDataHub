SELECT COUNT(line.risk_reference) AS policytransactionwrittendeductions_count
FROM
    line
    JOIN line_risk_code
    ON line.line_id = line_risk_code.line_id
    JOIN layer_trust_fund
    ON line.layer_id = layer_trust_fund.layer_id
    JOIN layer_deduction
    ON line.layer_id = layer_deduction.layer_id
    LEFT JOIN lookup_deduction_type
    ON layer_deduction.deduction_code = lookup_deduction_type.deduction_code
    JOIN layer
    ON line.layer_id = layer.layer_id