package enstar.globaldatahub.cdccontrol.properties

import enstar.globaldatahub.common.exceptions.PropertyNotSetException
import enstar.globaldatahub.common.properties.{ CommandLinePropertyParser, GDHProperties }
import org.apache.spark.Logging

/**
 *  Properties class for CDCControl
 */
class ControlProperties( propertyMap : Map[String, String] )
    extends GDHProperties
    with Logging {

  //check the properties have been set correctly
  checkPropertiesSet()

  /**
   * Get the value of a property as a (Java) boolean.
   *
   * @param name the name of the property
   * @return the prperty value as a Java boolean.
   */
  override def getBooleanProperty( name : String ) : java.lang.Boolean =
    propertyMap( name ).toBoolean.asInstanceOf[java.lang.Boolean]

  /**
   * Get the string value of a property
   *
   * @param name then name of the property
   * @return the property value in string format.
   */
  override def getStringProperty( name : String ) : String =
    propertyMap( name )

  /**
   * Get the value of a property as a string array.
   *
   * @param name the name of the property
   * @return the property value as an array
   */
  override def getArrayProperty( name : String ) : Array[String] =
    propertyMap( name ).split( "_" )

  /**
   * Check all required properties have been set correctly
   */
  override def checkPropertiesSet() : Unit = {
    checkProperty( "spark.cdccontrol.path.sql", _.get.asInstanceOf[String] )
    checkProperty( "spark.cdccontrol.path.data.control.input", _.get.asInstanceOf[String] )
    checkProperty( "spark.cdccontrol.path.data.control.output", _.get.asInstanceOf[String] )
    checkProperty( "spark.cdccontrol.path.data.input", _.get.asInstanceOf[String] )
    checkProperty( "spark.cdccontrol.tables.control.name", _.get.asInstanceOf[String] )
    checkProperty( "spark.cdccontrol.tables.temp.name", _.get.asInstanceOf[String] )
  }

  /**
   * Check that a property has been set correctly.
   *
   * @param keyName the property name
   * @param typeCheck a function to determine the type is correct.
   */
  private def checkProperty( keyName : String,
                             typeCheck : Option[Any] => Unit ) : Unit = {
    logDebug( "Checking property: " + keyName )
    if ( propertyMap.get( keyName ).isEmpty ) {
      throw new PropertyNotSetException( keyName, None )
    }
    try {
      val a = propertyMap.get( keyName )
      typeCheck( a )
    } catch {
      case e : Exception =>
        throw new PropertyNotSetException( "Wrong type: " + keyName, Some( e ) )
    }
    logDebug( "property " + keyName + " is valid" )
  }
}

object ControlProperties
    extends CommandLinePropertyParser
    with Logging {

  /**
   * command line parser
   */
  val parser = new scopt.OptionParser[Config]( "ControlJob" ) {
    head( "CDCControl", "0.1" )
    opt[Map[String, String]]( "ctrlOptions" )
      .valueName(
        "spark.cdccontrol.tables.control.name=v,etc..."
      )
      .required()
      .unbounded()
      .action { ( x, c ) =>
        c.copy( kwArgs = x )
      }
    note(
      "The following arguments are required:" +
        "\n\n" +
        "spark.cdccontrol.path.sql" +
        "\tThe path to the file containing the sql statement for the control processor.\n" +
        "spark.cdccontrol.path.data.control.input" +
        "\tThe path to the control table\n" +
        "spark.cdccontrol.path.data.control.output" +
        "\tThe path to write control data to\n" +
        "spark.cdccontrol.path.data.input" +
        "\tThe path to the data to be parsed\n" +
        "spark.cdccontrol.tables.control.name" +
        "\tThe name of the control table\n" +
        "spark.cdccontrol.tables.temp.name" +
        "\tThe name of the table being processed\n" +
        "\n\n" +
        "Separate listed values with an underscore.  Ie 1_2_3_4"
    )
  }
}
