SELECT
	"WrittenPremiumOurShare" AS transactiontypecode,
	premium_type_code AS transactionsubtypecode,
	"NDEX" AS sourcesystemcode,
	"WrittenPremiumOurShare" AS transactiontypedescription,
	premium_type_desc AS transactionsubtypedescription,
	"Premium" AS group,
	false AS iscashtransactiontype
FROM
	lookup_premium_type