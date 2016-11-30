package enstar.globaldatahub.common.properties

import enstar.globaldatahub.common.exceptions.PropertyNotSetException
import org.apache.spark.Logging
import scopt.OptionParser

/**
 * Expected behaviour for a command line parser.
 */
trait CommandLinePropertyParser extends Logging {

  /**
   * configuration object, required by parser
   * @param kwArgs a property map.
   */
  case class Config(kwArgs: Map[String, String])

  def parser: OptionParser[Config]

  /**
   * Map an array of strings in k1=v1,k2=v2 format to a Map[String,String]
   *
   * @param propertyArray the string array to map
   * @return a Map of values
   */
  def parseProperties(propertyArray: Array[String]): Map[String, String] = {
    logInfo("Parsing command line args")
    parser.parse(propertyArray, Config(Map[String, String]())) match {
      case Some(config) => {
        logInfo("Got valid arguments, continuing")
        config.kwArgs
      }
      case None =>
        logError("could not parse command line arguments")
        throw new PropertyNotSetException(
          "Unable to parse command line options",
          null)
    }
  }
}
