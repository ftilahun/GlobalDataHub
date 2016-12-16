package enstar.cdcprocessor.properties

/**
 * Trait for reading property values.
 */
case class CDCProperties(
  idColumnName: String = "",
  transactionColumnName: String = "",
  changeSequenceColumnName: String = "",
  changeInputDir: String = "",
  activeOutput: String = "",
  attunityColumnPrefix: String = "") extends Serializable
