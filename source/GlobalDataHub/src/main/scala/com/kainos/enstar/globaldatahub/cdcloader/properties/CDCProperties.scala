package com.kainos.enstar.globaldatahub.cdcloader.properties

import com.kainos.enstar.globaldatahub.exceptions.PropertyNotSetException
import com.kainos.enstar.globaldatahub.properties.{
  CommandLinePropertyParser,
  GDHProperties
}

/**
 * Properties object. Provides type checked properties.
 *
 * @param propertyMap a map of the property values
 */
class CDCProperties( propertyMap : Map[String, String] ) extends GDHProperties {

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
  }

  /**
   * Check all required properties have been set correctly
   */
  override def checkPropertiesSet() : Unit = {

    //known properties.
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
    checkProperty( "spark.cdcloader.paths.data.basedir",
      _.get.asInstanceOf[String] )
    checkProperty( "spark.cdcloader.paths.data.control",
      _.get.asInstanceOf[String] )
    checkProperty( "spark.cdcloader.paths.data.outputbasedir",
      _.get.asInstanceOf[String] )
    checkProperty( "spark.cdcloader.paths.sql.basedir",
      _.get.asInstanceOf[String] )
    checkProperty( "spark.cdcloader.paths.sql.control",
      _.get.asInstanceOf[String] )
    checkProperty( "spark.cdcloader.tables.control.name",
      _.get.asInstanceOf[String] )
    checkProperty( "spark.cdcloader.control.changemask.enabled",
      _.get.asInstanceOf[String] )
    checkProperty( "spark.cdcloader.input.tablenames",
      _.get.asInstanceOf[String].split( "," ) )
    checkProperty("spark.cdcloader.paths.data.outdir",
      _.get.asInstanceOf[String])
    //per table properties, determined at runtime.
    getArrayProperty( "spark.cdcloader.input.tablenames" ).foreach { tableName =>
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
object CDCProperties extends CommandLinePropertyParser {

  /**
   * configuration object, required by parser
   * @param kwArgs a property map.
   */
  case class Config( kwArgs : Map[String, String] )

  /**
   * command line parser
   */
  private val parser = new scopt.OptionParser[Config]( "scopt" ) {
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
      "The following options are required:" +
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
        "spark.cdcloader.paths.data.basedir\n" +
        "\t - The input base directory (e.g. /etl/cdc/attunity/ndex/)\n" +
        "spark.cdcloader.paths.data.control\n" +
        "\t - The path to the control table\n" +
        "spark.cdcloader.paths.data.outputbasedir\n" +
        "\t - The path to the output directory (e.g. /etl/cdc/cdcloader/ndex/)\n" +
        "spark.cdcloader.paths.data.outdir\n" +
        "\t - the name of the folder to write to (e.g. processing\n" +
        "spark.cdcloader.paths.sql.basedir\n" +
        "\t - The base directory for sql queries (e.g. /metadata/cdcloader/hive/queries/ndex/\n" +
        "spark.cdcloader.paths.sql.control\n" +
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

  /**
   * Map an array of strings in k1=v1,k2=v2 format to a Map[String,String]
   *
   * @param propertyArray the string array to map
   * @return a Map of values
   */
  override def parseProperties(
    propertyArray : Array[String] ) : Map[String, String] = {

    parser.parse( propertyArray, Config( Map[String, String]() ) ) match {
      case Some( config ) => {
        config.kwArgs
      }
      case None =>
        throw new PropertyNotSetException(
          "Unable to parse command line options",
          None )
    }
  }
}
