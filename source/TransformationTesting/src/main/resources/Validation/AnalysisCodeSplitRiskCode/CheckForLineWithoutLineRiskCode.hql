SELECT
    line.line_id
FROM line
    LEFT JOIN line_risk_code
    ON line.line_id = line_risk_code.line_id
    WHERE line_risk_code.line_id IS NULL