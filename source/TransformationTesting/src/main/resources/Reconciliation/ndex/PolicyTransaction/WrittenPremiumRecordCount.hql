SELECT
    COUNT(line.risk_reference) AS policytransaction_count
FROM
    line
    JOIN layer
    ON layer.layer_id = line.layer_id
    LEFT JOIN line_risk_code
    ON line.line_id = line_risk_code.line_id
    LEFT JOIN layer_trust_fund
    ON line.layer_id = layer_trust_fund.layer_id