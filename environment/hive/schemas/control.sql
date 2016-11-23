DROP SCHEMA IF EXISTS cdccontrol CASCADE;
CREATE SCHEMA IF NOT EXISTS cdccontrol;
DROP TABLE IF EXISTS cdccontrol.ndex;


CREATE EXTERNAL TABLE IF NOT EXISTS cdccontrol.ndex
STORED AS AVRO
LOCATION '/etl/cdc/cdcloader/control/ndex'
TBLPROPERTIES('avro.schema.url'='/metadata/cdcloader/control/ndex/control_schema.avsc');

CREATE INDEX IDX_Control_AttunityChangeSeq ON TABLE cdccontrol.ndex (attunitychangeseq) AS 'COMPACT' WITH DEFERRED REBUILD STORED AS AVRO;
ALTER INDEX IDX_Control_AttunityChangeSeq ON cdccontrol.ndex REBUILD;
ANALYZE TABLE cdccontrol.ndex COMPUTE STATISTICS FOR COLUMNS;

