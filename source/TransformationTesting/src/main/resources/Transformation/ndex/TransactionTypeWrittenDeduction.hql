SELECT
	"WrittenDeductionsOurShare" AS transactiontypecode,
	deduction_code AS transactionsubtypecode,
	"NDEX" AS sourcesystemcode,
	"WrittenDeductionsOurShare" AS transactiontypedescription,
	deduction_description AS transactionsubtypedescription,
	"Deduction" AS group,
	false AS iscashtransactiontype
FROM
	lookup_deduction_type