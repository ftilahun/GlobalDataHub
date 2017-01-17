package enstar.cdcprocessor.properties

/**
  * Defines exepected behaviour for a properties object
  */
trait PropertiesToMap {

  /**
    * Covert the properties object to an array of strings
    * @return
    */
  def toStringArray: Array[String]
}
