SELECT

line.risk_reference AS policynumber, 
CAST(line.line_id AS STRING) AS coveragereference,
CAST(line.layer_id AS STRING) AS sectionreference,
"NDEX" AS sourcesystemcode, 
ltrim(CASE regexp_extract(profit_centre_desc, '((?:Syndicate 1301)|(?:TIE)|(?:TIUK))(.*)', 2)
        WHEN '' THEN 'London'
        ELSE regexp_extract(profit_centre_desc, '((?:Syndicate 1301)|(?:TIE)|(?:TIUK))(.*)', 2)
    END) AS branchcode,
ltrim(CASE regexp_extract(profit_centre_desc, '((?:Syndicate 1301)|(?:TIE)|(?:TIUK))(.*)', 2)
        WHEN '' THEN 'London'
        ELSE regexp_extract(profit_centre_desc, '((?:Syndicate 1301)|(?:TIE)|(?:TIUK))(.*)', 2)
    END) AS branchdescription,
CASE line.line_status
	WHEN 'C' THEN layer.expiry_date
    ELSE CAST(NULL AS STRING)
    END AS cancellationdate,
layer.inception_date AS coverageeffectivefromdate, 
layer.expiry_date AS coverageeffectivetodate,
layer.expiry_date AS expirydate,
layer.fil_code AS filcode, 
layer.inception_date AS inceptiondate,
organisation.domicile_country_code AS insureddomicilecode,
CAST(NULL AS STRING) AS legacypolicynumber,
regexp_extract(lookup_profit_centre.profit_centre_desc, '(^(TIE)|(TIUK)|(Syndicate 1301))', 0) AS legalentitycode,
line.block AS lineofbusinesscode,
lookup_block.description AS lineofbusinessdescription,
line.risk_reference AS linkedmasterreference, 
line.risk_code AS majorriskcode, 
CAST(NULL AS STRING) AS majortrustfundcode,
CAST(line.business_type AS STRING) AS methodofplacementcode,
lookup_business_type.type_description AS methodofplacementdescription,
risk.area_code AS risklocationcode,
line.risk_reference AS sourcesystempolicynumber,
line.subblock AS sublineofbusinesscode, 
underwriting_block.description AS sublineofbusinessdescription, 
layer.unique_market_ref AS uniquemarketreference,
YEAR(layer.inception_date) AS yearofaccount,
line.line_status AS policystatuscode

FROM

line 
JOIN layer
ON line.layer_id = layer.layer_id
JOIN submission
ON layer.submission_id = submission.submission_id
JOIN risk
ON risk.risk_id = submission.risk_id
AND risk.programme_year = submission.programme_year
AND risk.sequence_no = submission.sequence_no
JOIN organisation
ON organisation.organisation_id = risk.assured_id
LEFT JOIN lookup_profit_centre
ON line.profit_centre_code = lookup_profit_centre.profit_centre_code
LEFT JOIN lookup_block
ON line.block = lookup_block.block
LEFT JOIN underwriting_block
ON line.block = underwriting_block.block
AND line.subblock = underwriting_block.subblock
LEFT JOIN lookup_business_type
ON lookup_business_type.business_type = line.business_type
WHERE line.line_status IN ('I', 'S', 'U', 'L', 'N', 'Q','P','D')