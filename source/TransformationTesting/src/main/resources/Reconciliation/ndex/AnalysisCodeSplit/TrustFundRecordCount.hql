SELECT
    COUNT(line.risk_reference) AS recordcount
FROM
    line JOIN layer_trust_fund
    ON line.layer_id = layer_trust_fund.layer_id
    JOIN lookup_trust_fund
    ON layer_trust_fund.trust_fund_indicator = lookup_trust_fund.trust_fund_indicator