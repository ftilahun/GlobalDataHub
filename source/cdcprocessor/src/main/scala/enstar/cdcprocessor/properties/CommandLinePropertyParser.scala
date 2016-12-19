package enstar.cdcprocessor.properties

import enstar.cdcprocessor.exceptions.PropertyNotSetException
import org.apache.spark.Logging
import scopt.OptionParser

/**
 * Parses command line properties.
 */
class CommandLinePropertyParser extends PropertyParser[Array[String]] with Logging {

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
        .text("The name of the id column")
      opt[String]("transactionColumnName")
        .required()
        .action(
          (t, p) => p.copy(transactionColumnName = t)
        )
        .text("The name of the transaction column")
      opt[String]("changeSequenceColumnName")
        .required()
        .action(
          (c, p) => p.copy(changeSequenceColumnName = c)
        )
        .text("The name of the change sequence column name")
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
        .text("The name of the change operation column")
      opt[String]("operationColumnValueBefore")
        .required()
        .action(
          (o, p) => p.copy(operationColumnValueBefore = o)
        )
        .text("The value of beforeimage rows in the operationColumnName")
      opt[String]("operationColumnValueInsert")
        .required()
        .action(
          (o, p) => p.copy(operationColumnValueInsert = o)
        )
        .text("The value of beforeimage rows in the operationColumnName")
      opt[String]("operationColumnValueUpdate")
        .required()
        .action(
          (o, p) => p.copy(operationColumnValueUpdate = o)
        )
        .text("The value of beforeimage rows in the operationColumnName")
      opt[String]("operationColumnValueDelete")
        .required()
        .action(
          (o, p) => p.copy(operationColumnValueDelete = o)
        )
        .text("The value of beforeimage rows in the operationColumnName")
      opt[String]("validToColumnName")
        .required()
        .action(
          (o, p) => p.copy(validToColumnName = o)
        )
        .text("The name of the valid to column")
      opt[String]("validFromColumnName")
        .required()
        .action(
          (o, p) => p.copy(validFromColumnName = o)
        )
        .text("The name of the valid to column")
      opt[String]("transactionTimeStampColumnName")
        .required()
        .action(
          (o, p) => p.copy(transactionTimeStampColumnName = o)
        )
        .text("The name of the header timestamp column")
    }

  def parse(commandLineArgs: Array[String]): CDCProperties = {
    parser.parse(commandLineArgs, CDCProperties()) match {
      case Some(properties) => properties
      case None             => throw new PropertyNotSetException()
    }
  }
}
