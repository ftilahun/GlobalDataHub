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
                         attunityCutoff: String = "",
                         attunityDateFormat: String = "",
                         attunityDateFormatShort: String = "",
                         historyInput: String = "",
                         immatureChangesOutput: String = "",
                         historyOutput: String = "",
                         printStatistics: Boolean = false)
    extends Serializable