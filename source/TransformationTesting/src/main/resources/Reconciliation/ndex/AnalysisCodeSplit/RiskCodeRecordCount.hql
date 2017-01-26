SELECT
    COUNT(line.risk_reference) AS recordcount
FROM
    line JOIN line_risk_code
    ON line.line_id = line_risk_code.line_id
    JOIN lookup_risk_code
    ON line_risk_code.risk_code = lookup_risk_code.risk_code