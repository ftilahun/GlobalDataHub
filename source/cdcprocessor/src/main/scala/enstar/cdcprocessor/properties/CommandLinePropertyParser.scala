package enstar.cdcprocessor.properties

import enstar.cdcprocessor.exceptions.PropertyNotSetException
import org.apache.spark.Logging
import scopt.OptionParser

/**
 * Parses command line properties.
 */
class CommandLinePropertyParser extends Logging {

  val parser: OptionParser[CDCProperties] = new scopt.OptionParser[CDCProperties]("cdcprocessor") {
    head("cdcprocessor", "0.3")

    opt[String]("changeInput").
      required().
      action(
        (i, p) => p.copy(changeInputDir = i)
      ).
        text("The input directory for change data")

    opt[String]("activeOutput").
      required().
      action(
        (i, p) => p.copy(activeOutput = i)
      ).
        text("The output directory for active data")

    opt[String]("idColumnName").
      required().
      action(
        (i, p) => p.copy(idColumnName = i)
      ).
        text("The name of the id column")
    opt[String]("transactionColumnName").
      required().
      action(
        (t, p) => p.copy(transactionColumnName = t)
      ).
        text("The name of the transaction column")
    opt[String]("changeSequenceColumnName").
      required().
      action(
        (c, p) => p.copy(changeSequenceColumnName = c)
      ).
        text("The name of the change sequence column name")
    opt[String]("attunityColumnPrefix").
      required().
      action(
        (a, p) => p.copy(attunityColumnPrefix = a)
      ).
        text("The prefix used by attunity columns (e.g. header__)")
  }

  def parse(commandLineArgs: Array[String]): CDCProperties = {
    parser.parse(commandLineArgs, CDCProperties()) match {
      case Some(properties) => properties
      case None             => throw new PropertyNotSetException()
    }
  }
}
