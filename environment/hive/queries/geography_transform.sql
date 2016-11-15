SELECT

country_code AS geographycode,
'NDEX' AS sourcesystemcode,
'Country' AS geographytypecode,
null AS parentgeographycode,
null AS parentsourcesystemcode,
country_name AS geographydescription,
_isdeleted,
_changeoperation,
_loaddatetimestamp,
_loadidentifier

FROM

lookup_country