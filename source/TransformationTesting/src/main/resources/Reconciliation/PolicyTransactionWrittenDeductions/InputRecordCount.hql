SELECT COUNT(*) AS policytransactionwrittendeductions_count
FROM
    line
    JOIN settlement_schedule
    ON line.layer_id = settlement_schedule.layer_id
    JOIN line_risk_code
    ON line.line_id = line_risk_code.line_id
    JOIN layer_trust_fund
    ON line.layer_id = layer_trust_fund.layer_id
    JOIN layer_deduction
    ON line.layer_id = layer_deduction.layer_id
    JOIN lookup_risk_code
    ON line_risk_code.risk_code = lookup_risk_code.risk_code
    LEFT JOIN lookup_deduction_type
    ON layer_deduction.deduction_code = lookup_deduction_type.deduction_code
    JOIN lookup_trust_fund
    ON layer_trust_fund.trust_fund_indicator = lookup_trust_fund.trust_fund_indicator
    JOIN layer
    ON line.layer_id = layer.layer_id