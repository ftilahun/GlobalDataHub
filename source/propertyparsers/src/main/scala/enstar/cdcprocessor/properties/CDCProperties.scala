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
                         metricsOutputDir: Option[String] = None)
    extends Serializable with PropertiesToMap {

  /**
   * Covert the properties object to a Map of String -> String
   * @return a map of property values
   */
  def toMap: Map[String, String] = {
    val optionalArgs: Map[String, String] = if (metricsOutputDir.isDefined) {
      Map[String, String]("--metricsOutputDir" -> metricsOutputDir.get)
    } else {
      Map[String, String]()
    }

    Map[String, String](
      "--transactionTimeStampColumnName" -> transactionTimeStampColumnName,
      "--operationColumnName" -> operationColumnName,
      "--operationColumnValueBefore" -> operationColumnValueBefore,
      "--operationColumnValueInsert" -> operationColumnValueInsert,
      "--operationColumnValueUpdate" -> operationColumnValueUpdate,
      "--operationColumnValueDelete" -> operationColumnValueDelete,
      "--transactionIdColumnName" -> transactionIdColumnName,
      "--changeSequenceColumnName" -> changeSequenceColumnName,
      "--changeInputDir" -> changeInputDir,
      "--activeOutput" -> activeOutput,
      "--attunityColumnPrefix" -> attunityColumnPrefix,
      "--validFromColumnName" -> validFromColumnName,
      "--validToColumnName" -> validToColumnName,
      "--activeColumnName" -> activeColumnName,
      "--attunityCutoff" -> attunityCutoff,
      "--attunityDateFormat" -> attunityDateFormat,
      "--attunityDateFormatShort" -> attunityDateFormatShort,
      "--historyInput" -> historyInput,
      "--immatureChangesOutput" -> immatureChangesOutput,
      "--historyOutput" -> historyOutput) ++ optionalArgs
  }
}
