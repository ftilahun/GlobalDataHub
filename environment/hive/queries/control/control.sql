SELECT 
	MAX(header__change_seq) AS attunitychangeseq, 
	MAX(_loadtimestamp) AS starttime, 
	getCurrentTime() AS endtime,
	_tablename AS attunitytablename,
	COUNT(_tablename) AS recordcount 
FROM control
GROUP BY _tablename