SELECT

line.risk_reference AS policynumber, 
line.line_id AS coveragereference, 
line.layer_id AS sectionreference,
"NDEX" AS sourcesystemcode, 
line.profit_centre_code AS branchcode, 
lookup_profit_centre.profit_centre_desc AS branchdescription,
CASE line.line_status
	WHEN 'C' THEN layer.expiry_date
    ELSE CAST(NULL AS STRING)
    END AS cancellationdate,
layer.inception_date AS coverageeffectivefromdate, 
layer.expiry_date AS coverageeffectivetodate,
layer.expiry_date AS expirydate,
layer.fil_code AS filcode, 
layer.inception_date AS inceptiondate,
CAST(NULL AS STRING) AS legacypolicynumber,
line.profit_centre_code AS legalentitycode, 
line.block AS lineofbusinesscode,
lookup_block.description AS lineofbusinessdescription,
line.risk_reference AS linkedmasterreference, 
line.risk_code AS majorriskcode, 
CAST(NULL AS STRING) AS majortrustfundcode, 
line.risk_reference AS sourcesystempolicynumber, 
line.subblock AS sublineofbusinesscode, 
underwriting_block.description AS sublineofbusinessdescription, 
layer.unique_market_ref AS uniquemarketreference,
YEAR(layer.inception_date) AS yearofaccount

FROM

line 
JOIN layer
ON line.line_id = layer.layer_id
JOIN lookup_profit_centre 
ON line.profit_centre_code = lookup_profit_centre.profit_centre_code
JOIN lookup_block
ON line.block = lookup_block.block
JOIN underwriting_block
ON lookup_block.block = underwriting_block.block 