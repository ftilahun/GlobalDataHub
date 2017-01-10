package enstar.cdcprocessor.properties

/**
 * Properties class
 */
case class CDCProperties(idColumnName: String = "",
                         transactionTimeStampColumnName: String = "",
                         operationColumnName: String = "",
                         operationColumnValueBefore: String = "B",
                         operationColumnValueInsert: String = "I",
                         operationColumnValueUpdate: String = "U",
                         operationColumnValueDelete: String = "D",
                         transactionIdColumnName: String = "",
                         changeSequenceColumnName: String = "",
                         changeInputDir: String = "",
                         activeOutput: String = "",
                         attunityColumnPrefix: String = "",
                         validFromColumnName: String = "",
                         validToColumnName: String = "",
                         activeColumnName: String = "",
                         timeWindowInHours: Int = 0,
                         attunityDateFormat: String = "",
                         historyInput: String = "",
                         immatureChangesOutput: String = "",
                         historyOutput: String = "")
    extends Serializable {}
