package enstar.cdctableprocessor.properties

/**
  * Properties class
  */
case class CDCProperties(
                         //common properties for all tables
                         idColumnName: String = "",
                         transactionTimeStampColumnName: String = "",
                         operationColumnName: String = "",
                         operationColumnValueBefore: String = "B",
                         operationColumnValueInsert: String = "I",
                         operationColumnValueUpdate: String = "U",
                         operationColumnValueDelete: String = "D",
                         transactionIdColumnName: String = "",
                         changeSequenceColumnName: String = "",
                         attunityColumnPrefix: String = "",
                         validFromColumnName: String = "",
                         validToColumnName: String = "",
                         activeColumnName: String = "",
                         attunityCutoff: String = "",
                         attunityDateFormat: String = "",
                         attunityDateFormatShort: String = "",
                         //per table properties
                         changeInputDir: String = "",
                         activeInput: String = "",
                         activeOutput: String = "",
                         immatureChangesOutput: String = "",
                         historyOutput: String = "",
                         metricsOutputDir: Option[String] = None)
    extends Serializable
    with PropertiesToMap {

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
      "--historyInput" -> activeInput,
      "--immatureChangesOutput" -> immatureChangesOutput,
      "--historyOutput" -> historyOutput
    ) ++ optionalArgs
  }

  /**
    * Create a new CDCProperties object with the supplied input/output directories
    *
    * @param changeInputDir The directory to read new changes from
    * @param activeInput The directory to read the previously active records from
    * @param activeOutput the diretory to write active records to
    * @param immatureChangesOutput the directory to write rejected changes to
    * @param historyOutput the directory to write closed records to
    * @param metricsOutputDir the optional directory to write metrics to
    * @return
    */
  def updateDirectories(changeInputDir: String,
                 activeInput: String,
                 activeOutput: String,
                 immatureChangesOutput: String,
                 historyOutput: String,
                 metricsOutputDir: Option[String]): CDCProperties = {
    this.copy(
      changeInputDir = changeInputDir,
      activeInput = activeInput,
      immatureChangesOutput = immatureChangesOutput,
      historyOutput = historyOutput,
      metricsOutputDir = metricsOutputDir
    )
  }
}
