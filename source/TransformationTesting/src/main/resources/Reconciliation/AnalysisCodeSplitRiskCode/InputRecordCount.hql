SELECT
    COUNT(*)
FROM line
    INNER JOIN line_risk_code
    ON line.line_id = line_risk_code.line_id