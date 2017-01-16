package enstar.cdcprocessor.properties

import enstar.cdcprocessor.exceptions.PropertyNotSetException
import scopt.OptionParser

/**
 * Parses command line properties.
 */
class CommandLinePropertyParser
    extends PropertyParser[Array[String]] {

  val parser: OptionParser[CDCProperties] =
    new scopt.OptionParser[CDCProperties]("cdcprocessor") {
      head("cdcprocessor", "0.3")

      opt[String]("changeInput")
        .required()
        .action(
          (i, p) => p.copy(changeInputDir = i)
        )
        .text("The input directory for change data")

      opt[String]("activeOutput")
        .required()
        .action(
          (i, p) => p.copy(activeOutput = i)
        )
        .text("The output directory for active data")

      opt[String]("idColumnName")
        .required()
        .action(
          (i, p) => p.copy(idColumnName = i)
        )
        .text("The name of the id column for this table (e.g. header__id)")

      opt[String]("transactionIdColumnName")
        .required()
        .action(
          (t, p) => p.copy(transactionIdColumnName = t)
        )
        .text("The name of the transaction column (provided by attunity, e.g. header__transaction_id)")

      opt[String]("changeSequenceColumnName")
        .required()
        .action(
          (c, p) => p.copy(changeSequenceColumnName = c)
        )
        .text("The name of the change sequence column (provided by attunity e.g. header__change_seq)")

      opt[String]("attunityColumnPrefix")
        .required()
        .action(
          (a, p) => p.copy(attunityColumnPrefix = a)
        )
        .text("The prefix used by attunity columns (e.g. header__)")

      opt[String]("operationColumnName")
        .required()
        .action(
          (o, p) => p.copy(operationColumnName = o)
        )
        .text("The name of the change operation column (provided by attunity e.g. header__change_oper)")

      opt[String]("operationColumnValueBefore")
        .required()
        .action(
          (o, p) => p.copy(operationColumnValueBefore = o)
        )
        .text("The value of before rows in the operationColumnName e.g. 'B' ")

      opt[String]("operationColumnValueInsert")
        .required()
        .action(
          (o, p) => p.copy(operationColumnValueInsert = o)
        )
        .text("The value of insert rows in the operationColumnName e.g. 'I' ")

      opt[String]("operationColumnValueUpdate")
        .required()
        .action(
          (o, p) => p.copy(operationColumnValueUpdate = o)
        )
        .text("The value of update rows in the operationColumnName e.g. 'U'")

      opt[String]("operationColumnValueDelete")
        .required()
        .action(
          (o, p) => p.copy(operationColumnValueDelete = o)
        )
        .text("The value of update rows in the operationColumnName e.g. 'U'")

      opt[String]("validToColumnName")
        .required()
        .action(
          (o, p) => p.copy(validToColumnName = o)
        )
        .text("The name of the valid to column e.g. validto")

      opt[String]("validFromColumnName")
        .required()
        .action(
          (o, p) => p.copy(validFromColumnName = o)
        )
        .text("The name of the valid from column e.g. valdfrom")

      opt[String]("transactionTimeStampColumnName")
        .required()
        .action(
          (o, p) => p.copy(transactionTimeStampColumnName = o)
        )
        .text("The name of the transaction timestamp column (provided by attunity e.g. header__timestamp)")
      opt[String]("activeColumnName")
        .required()
        .action(
          (a, p) => p.copy(activeColumnName = a)
        )
        .text("The name of the active column e.g. 'active'")
      opt[String]("attunityCutoff")
        .required()
        .action(
          (t, p) => p.copy(attunityCutoff = t)
        )
        .text("The timestamp prior to which changes will not be processed (must be in attunityDateFormat)")

      opt[String]("attunityDateFormat")
        .required()
        .action(
          (a, p) => p.copy(attunityDateFormat = a)
        )
        .text("The time format for the attuntiy timestamp column (e.g. YYYY/MM/DD HH:mm:ss.SSS)")

      opt[String]("attunityDateFormatShort")
        .required()
        .action(
          (a, p) => p.copy(attunityDateFormatShort = a)
        )
        .text(
          "The time format for the attuntiy timestamp column without milliseconds" +
            " (e.g. YYYY/MM/DD HH:mm:ss)")

      opt[String]("historyInput")
        .required()
        .action(
          (h, p) => p.copy(historyInput = h)
        )
        .text("The input directory for history data")

      opt[String]("immatureChangesOutput")
        .required()
        .action(
          (i, p) => p.copy(immatureChangesOutput = i)
        )
        .text("The output directory for changes too young to process")

      opt[String]("historyOutput")
        .required()
        .action(
          (h, p) => p.copy(historyOutput = h)
        )
        .text("The output directory for closed records")

      opt[String]("metricsOutputDir")
        .action(
          (s, p) => p.copy(metricsOutputDir = Some(s))
        )
        .text("Directory to save job metrics to")
    }

  def parse(commandLineArgs: Array[String]): CDCProperties = {
    parser.parse(commandLineArgs, CDCProperties()) match {
      case Some(properties) => properties
      case None             => throw new PropertyNotSetException()
    }
  }
}
