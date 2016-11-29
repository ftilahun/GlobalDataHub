SELECT
    COUNT(*) AS record_count
FROM
    line
    JOIN layer
    ON layer.layer_id = line.layer_id
    JOIN settlement_schedule
    ON line.layer_id = settlement_schedule.layer_id
    JOIN line_risk_code
    ON line.line_id = line_risk_code.line_id
    JOIN layer_trust_fund
    ON line.layer_id = layer_trust_fund.layer_id
    JOIN lookup_premium_type
    ON lookup_premium_type.premium_type_code = settlement_schedule.premium_type_code