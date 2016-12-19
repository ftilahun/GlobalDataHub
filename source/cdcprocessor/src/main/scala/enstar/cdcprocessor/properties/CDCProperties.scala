package enstar.cdcprocessor.properties

/**
 * Properties class
 */
case class CDCProperties(idColumnName: String = "",
                         transactionColumnName: String = "",
                         transactionTimeStampColumnName: String = "",
                         operationColumnName: String = "",
                         operationColumnValueBefore: String = "B",
                         operationColumnValueInsert: String = "I",
                         operationColumnValueUpdate: String = "U",
                         operationColumnValueDelete: String = "D",
                         changeSequenceColumnName: String = "",
                         changeInputDir: String = "",
                         activeOutput: String = "",
                         attunityColumnPrefix: String = "",
                         validFromColumnName: String = "",
                         validToColumnName: String = "")
    extends Serializable
