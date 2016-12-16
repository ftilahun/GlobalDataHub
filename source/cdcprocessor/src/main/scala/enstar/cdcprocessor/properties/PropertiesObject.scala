package enstar.cdcprocessor.properties

/**
  * Trait for reading property values.
  */
trait PropertiesObject extends Serializable {

  /**
    * Get the value of a property as a (Java) boolean.
    *
    * @param name the name of the property
    * @return the prperty value as a Java boolean.
    */
  def getBooleanProperty(name: String): java.lang.Boolean

  /**
    * Get the string value of a property
    *
    * @param name then name of the property
    * @return the property value in string format.
    */
  def getStringProperty(name: String): String

  /**
    * Get the value of a property as a string array.
    *
    * @param name the name of the property
    * @return the property value as an array
    */
  def getArrayProperty(name: String): Array[String]

  /**
    * Check all required properties have been set correctly
    */
  def checkPropertiesSet(): Unit
}