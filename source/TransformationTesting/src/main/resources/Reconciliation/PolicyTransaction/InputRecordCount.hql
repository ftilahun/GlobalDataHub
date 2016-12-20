SELECT
    COUNT(line.risk_reference) AS record_count
FROM
    line
    JOIN layer
    ON layer.layer_id = line.layer_id
    JOIN line_risk_code
    ON line.line_id = line_risk_code.line_id
    JOIN layer_trust_fund
    ON line.layer_id = layer_trust_fund.layer_id