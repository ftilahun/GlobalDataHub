package enstar.globaldatahub.cdcloader.properties

import enstar.globaldatahub.common.exceptions.PropertyNotSetException
import enstar.globaldatahub.common.properties.{CommandLinePropertyParser, GDHProperties}
import org.apache.spark.Logging

/**
 * Properties object. Provides type checked properties.
 *
 * @param propertyMap a map of the property values
 */
class CDCProperties( propertyMap : Map[String, String] )
    extends GDHProperties
    with Logging {

  //check the properties have been set correctly
  checkPropertiesSet()

  /**
   * Get the string value of a property
   *
   * @param name then name of the property
   * @return the property value in string format.
   */
  override def getStringProperty( name : String ) : String = propertyMap( name )

  /**
   * Get the value of a property as a string array.
   *
   * @param name the name of the property
   * @return the property value as an array
   */
  override def getArrayProperty( name : String ) : Array[String] =
    propertyMap( name ).split( "_" )

  /**
   * Get the value of a property as a (Java) boolean.
   *
   * @param name the name of the property
   * @return the prperty value as a Java boolean.
   */
  override def getBooleanProperty( name : String ) : java.lang.Boolean =
    propertyMap( name ).toBoolean.asInstanceOf[java.lang.Boolean]

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

  /**
   * Check all required properties have been set correctly
   */
  override def checkPropertiesSet() : Unit = {

    logInfo( "Checking standard properties" )
    checkProperty( "spark.cdcloader.columns.attunity.name.changemask",
      _.get.asInstanceOf[String] )
    checkProperty( "spark.cdcloader.columns.attunity.name.changeoperation",
      _.get.asInstanceOf[String] )
    checkProperty( "spark.cdcloader.columns.attunity.name.changesequence",
      _.get.asInstanceOf[String] )
    checkProperty( "spark.cdcloader.columns.attunity.value.changeoperation",
      _.get.asInstanceOf[String] )
    checkProperty( "spark.cdcloader.columns.control.names.controlcolumnnames",
      _.get.asInstanceOf[String] )
    checkProperty( "spark.cdcloader.columns.metadata.name.isdeleted",
      _.get.toString.toBoolean == true )
    checkProperty( "spark.cdcloader.control.attunity.changetablesuffix",
      _.get.asInstanceOf[String] )
    checkProperty( "spark.cdcloader.columns.metadata.name.loadtimestamp",
      _.get.asInstanceOf[String] )
    checkProperty( "spark.cdcloader.format.timestamp.attunity",
      _.get.asInstanceOf[String] )
    checkProperty( "spark.cdcloader.format.timestamp.hive",
      _.get.asInstanceOf[String] )
    checkProperty( "spark.cdcloader.path.data.basedir",
      _.get.asInstanceOf[String] )
    checkProperty( "spark.cdcloader.path.data.control",
      _.get.asInstanceOf[String] )
    checkProperty( "spark.cdcloader.path.data.outputbasedir",
      _.get.asInstanceOf[String] )
    checkProperty( "spark.cdcloader.path.sql.basedir",
      _.get.asInstanceOf[String] )
    checkProperty( "spark.cdcloader.path.sql.control",
      _.get.asInstanceOf[String] )
    checkProperty( "spark.cdcloader.tables.control.name",
      _.get.asInstanceOf[String] )
    checkProperty( "spark.cdcloader.control.changemask.enabled",
      _.get.asInstanceOf[String] )
    checkProperty( "spark.cdcloader.input.tablenames",
      _.get.asInstanceOf[String].split( "," ) )
    checkProperty( "spark.cdcloader.path.data.outdir",
      _.get.asInstanceOf[String] )
    logInfo( "Checking per table properties." )
    logInfo( "EXpecting " + getArrayProperty( "spark.cdcloader.input.tablenames" ).
      length + " tables" )
    getArrayProperty( "spark.cdcloader.input.tablenames" ).foreach { tableName =>
      logInfo( "Checking properties for: " + tableName )
      checkProperty( "spark.cdcloader.control.columnpositions." + tableName,
        _.get.asInstanceOf[String].split( "," ) )
      checkProperty(
        "spark.cdcloader.columns.control.name.tablename." + tableName,
        _.get.asInstanceOf[String].split( "," ) )
    }

  }
}

/**
 * Companion class for CDCProperties
 */
object CDCProperties
    extends CommandLinePropertyParser {

  /**
   * command line parser
   */
  override def parser = new scopt.OptionParser[Config]( "CDCLoader" ) {
    head( "CDCLoader", "0.1" )
    opt[Map[String, String]]( "cdcOptions" )
      .valueName(
        "spark.cdcloader.columns.attunity.name.changemask=v,etc..."
      )
      .required()
      .unbounded()
      .action { ( x, c ) =>
        c.copy( kwArgs = x )
      }
    note(
      "The following arguments are required:" +
        "\n\n" +
        "spark.cdcloader.columns.attunity.name.changemask\n" +
        "\t - The name of the attunity change mask column\n" +
        "spark.cdcloader.columns.attunity.name.changeoperation\n" +
        "\t - The name of the change operation column\n" +
        "spark.cdcloader.columns.attunity.name.changesequence\n" +
        "\t - The name of the attunity change sequence column\n" +
        "spark.cdcloader.columns.control.names.controlcolumnnames\n" +
        "\t - A list of column names in the control table\n" +
        "spark.cdcloader.columns.attunity.value.changeoperation\n" +
        "\t - The value of the delete operation in the attunity change operation field\n" +
        "spark.cdcloader.columns.metadata.name.isdeleted\n" +
        "\t - The name to give the isDeleted attribute in the outputted avro\n" +
        "spark.cdcloader.control.attunity.changetablesuffix\n" +
        "\t - The suffix given to attunity change tables (e.g. __ct)\n" +
        "spark.cdcloader.columns.metadata.name.loadtimestamp\n" +
        "\t - The name to give the load timestamp attribute in the outputted avro\n" +
        "spark.cdcloader.format.timestamp.attunity\n" +
        "\t - The timestamp format for the attunity change sequence\n" +
        "spark.cdcloader.format.timestamp.hive\n" +
        "\t - The timestamp format to use for hive\n" +
        "spark.cdcloader.path.data.basedir\n" +
        "\t - The input base directory (e.g. /etl/cdc/attunity/ndex/)\n" +
        "spark.cdcloader.path.data.control\n" +
        "\t - The path to the control table\n" +
        "spark.cdcloader.path.data.outputbasedir\n" +
        "\t - The path to the output directory (e.g. /etl/cdc/cdcloader/ndex/)\n" +
        "spark.cdcloader.path.data.outdir\n" +
        "\t - the name of the folder to write to (e.g. processing\n" +
        "spark.cdcloader.path.sql.basedir\n" +
        "\t - The base directory for sql queries (e.g. /metadata/cdcloader/hive/queries/ndex/\n" +
        "spark.cdcloader.path.sql.control\n" +
        "\t - \"The path to the control table sql (e.g. /metadata/cdcloader/hive/queries/control/)\n" +
        "spark.cdcloader.tables.control.name\ns" +
        "\t - The name of the control table\n" +
        "spark.cdcloader.control.changemask.enabled\n" +
        "\t - Should the change mask be enabled(true/false)?\n" +
        "spark.cdcloader.input.tablenames\n" +
        "\t - A comma separated list of tables to process\n" +
        "\n" +
        "The following properties are required per input table:\n" +
        "spark.cdcloader.control.columnpositions\n" +
        "\t - A list of ordinal column positions in the *change table*\n" +
        "spark.cdcloader.columns.control.name.tablename\n" +
        "\t - The name of the table name field in the control table\n" +
        "\n" +
        "e.g. spark.cdcloader.columns.control.name.tablename.policy" +
        "\n\n" +
        "Separate listed values with an underscore.  Ie 1_2_3_4"
    )
  }
}
