package enstar.cdcprocessor.properties

/**
 * Properties class
 */
case class CDCProperties(idColumnName: String = "",
                         transactionColumnName: String = "",
                         changeSequenceColumnName: String = "",
                         changeInputDir: String = "",
                         activeOutput: String = "",
                         attunityColumnPrefix: String = "")
    extends Serializable
