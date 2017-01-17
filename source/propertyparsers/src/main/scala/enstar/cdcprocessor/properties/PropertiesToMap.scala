package enstar.cdcprocessor.properties

/**
 * Defines exepected behaviour for a properties object
 */
trait PropertiesToMap {

  /**
   * Covert the properties object to a Map of String -> String
   * @return a map of property values
   */
  def toMap: Map[String, String]
}
