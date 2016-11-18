package com.kainos.enstar.globaldatahub.cdcloader.properties

import com.kainos.enstar.globaldatahub.exceptions.PropertyNotSetException
import org.scalatest.{ FlatSpec, GivenWhenThen, Matchers }

/**
 * Unit tests for CDCProperties
 */
class CDCPropertiesSpec extends FlatSpec with GivenWhenThen with Matchers {

  "CDCProperties" should "Parse the command line arguments" in {
    Given( "A set of command line args" )
    val argsArray = Array[String](
      "--cdcOptions",
      "spark.cdcloader.columns.attunity.name.changemask=a," +
        "spark.cdcloader.columns.attunity.name.changeoperation=b," +
        "spark.cdcloader.columns.attunity.name.changesequence=c," +
        "spark.cdcloader.columns.attunity.value.changeoperation=d" )
    When( "Parsing arguments" )
    val props = CDCProperties.parseProperties( argsArray )
    Then( "Property values should be set " )
    props.get( "spark.cdcloader.columns.attunity.name.changemask" ) should be(
      Some( "a" ) )
    props.get( "spark.cdcloader.columns.attunity.name.changeoperation" ) should be(
      Some( "b" ) )
    props.get( "spark.cdcloader.columns.attunity.name.changesequence" ) should be(
      Some( "c" ) )
    props.get( "spark.cdcloader.columns.attunity.value.changeoperation" ) should be(
      Some( "d" ) )
  }

  "CDCProperties" should "Throw when unable to parse command line options" in {
    Given( "Invalid input" )
    When( "--cdcOptions has no arguments" )
    Then( "a PropertyNotSetException should be thrown" )
    an[PropertyNotSetException] should be thrownBy {
      CDCProperties.parseProperties( Array[String]( "--cdcOptions" ) )
    }
    Given( "Invalid input" )
    When( "--cdcOptions was not set" )
    Then( "a PropertyNotSetException should be thrown" )
    an[PropertyNotSetException] should be thrownBy {
      CDCProperties.parseProperties( Array[String]() )
    }
    Given( "Invalid input" )
    When( "Passed a null reference" )
    Then( "a NullPointerException should be thrown" )
    an[NullPointerException] should be thrownBy {
      CDCProperties.parseProperties( null )
    }
  }

  "CDCProperties" should "Check properties have been set" in {
    Given( "Valid input" )
    When( "Any individual property has not been set" )
    Then( "a PropertyNotSetException should be thrown" )
    an[PropertyNotSetException] should be thrownBy {
      new CDCProperties( CDCProperties.parseProperties( Array[String](
        "--cdcOptions",
        "spark.cdcloader.paths.sql.control=a"
      ) ) )
    }
    Given( "Valid input" )
    When( "Any individual property has the wrong type" )
    Then( "a PropertyNotSetException should be thrown" )
    an[PropertyNotSetException] should be thrownBy {
      new CDCProperties( CDCProperties.parseProperties( Array[String](
        "--cdcOptions",
        "spark.cdcloader.columns.attunity.name.changemask=a," +
          "spark.cdcloader.columns.control.names.controlcolumnnames=a," +
          "spark.cdcloader.columns.attunity.name.changeoperation=a," +
          "spark.cdcloader.columns.attunity.name.changesequence=a," +
          "spark.cdcloader.columns.attunity.value.changeoperation=a," +
          "spark.cdcloader.columns.metadata.name.isdeleted=a," +
          "spark.cdcloader.control.attunity.changetablesuffix=a," +
          "spark.cdcloader.columns.metadata.name.loadtimestamp=a," +
          "spark.cdcloader.format.timestamp.attunity=a," +
          "spark.cdcloader.format.timestamp.hive=a," +
          "spark.cdcloader.paths.data.basedir=a," +
          "spark.cdcloader.paths.data.control=a," +
          "spark.cdcloader.paths.data.output=a," +
          "spark.cdcloader.paths.data.outputbasedir=a," +
          "spark.cdcloader.paths.sql.basedir=a," +
          "spark.cdcloader.paths.sql.control=a," +
          "spark.cdcloader.tables.control.name=a," +
          "spark.cdcloader.control.changemask.enabled=a," +
          "spark.cdcloader.input.tablenames=a," +
          "spark.cdcloader.control.columnpositions.a=a," +
          "spark.cdcloader.columns.control.name.tablename.a=a"
      ) ) )
    }
    Given( "Valid input" )
    When( "All propertie have been set correctly" )
    Then( "all properties should have been set" )
    val props = new CDCProperties( CDCProperties.parseProperties( Array[String](
      "--cdcOptions",
      "spark.cdcloader.columns.attunity.name.changemask=a," +
        "spark.cdcloader.columns.control.names.controlcolumnnames=a," +
        "spark.cdcloader.columns.attunity.name.changeoperation=a," +
        "spark.cdcloader.columns.attunity.name.changesequence=a," +
        "spark.cdcloader.columns.attunity.value.changeoperation=a," +
        "spark.cdcloader.columns.metadata.name.isdeleted=true," +
        "spark.cdcloader.control.attunity.changetablesuffix=a," +
        "spark.cdcloader.columns.metadata.name.loadtimestamp=a," +
        "spark.cdcloader.paths.data.outputbasedir=a," +
        "spark.cdcloader.paths.data.outdir=a," +
        "spark.cdcloader.format.timestamp.attunity=a," +
        "spark.cdcloader.format.timestamp.hive=a," +
        "spark.cdcloader.paths.data.basedir=a," +
        "spark.cdcloader.paths.data.control=a," +
        "spark.cdcloader.paths.data.output=a," +
        "spark.cdcloader.paths.sql.basedir=a," +
        "spark.cdcloader.paths.sql.control=a," +
        "spark.cdcloader.tables.control.name=a," +
        "spark.cdcloader.control.changemask.enabled=a," +
        "spark.cdcloader.input.tablenames=a_b," +
        "spark.cdcloader.control.columnpositions.a=1_2_3_4," +
        "spark.cdcloader.columns.control.name.tablename.a=a," +
        "spark.cdcloader.control.columnpositions.b=1_2_3," +
        "spark.cdcloader.columns.control.name.tablename.b=b"
    ) ) )

    props.getStringProperty( "spark.cdcloader.columns.attunity.name.changemask" ) should be ( "a" )
    props.getStringProperty( "spark.cdcloader.columns.attunity.name.changeoperation" ) should be ( "a" )
    props.getStringProperty( "spark.cdcloader.columns.attunity.name.changesequence" ) should be ( "a" )
    props.getStringProperty( "spark.cdcloader.columns.attunity.value.changeoperation" ) should be ( "a" )
    props.getStringProperty( "spark.cdcloader.control.attunity.changetablesuffix" ) should be ( "a" )
    props.getStringProperty( "spark.cdcloader.columns.metadata.name.loadtimestamp" ) should be ( "a" )
    props.getStringProperty( "spark.cdcloader.format.timestamp.attunity" ) should be ( "a" )
    props.getStringProperty( "spark.cdcloader.format.timestamp.hive" ) should be ( "a" )
    props.getStringProperty( "spark.cdcloader.paths.data.basedir" ) should be ( "a" )
    props.getStringProperty( "spark.cdcloader.paths.data.control" ) should be ( "a" )
    props.getStringProperty( "spark.cdcloader.paths.data.output" ) should be ( "a" )
    props.getStringProperty( "spark.cdcloader.paths.sql.basedir" ) should be ( "a" )
    props.getStringProperty( "spark.cdcloader.paths.sql.control" ) should be ( "a" )
    props.getStringProperty( "spark.cdcloader.tables.control.name" ) should be ( "a" )
    props.getStringProperty( "spark.cdcloader.control.changemask.enabled" ) should be ( "a" )
    props.getStringProperty( "spark.cdcloader.columns.control.name.tablename.a" ) should be ( "a" )

    val tblNames = props.getArrayProperty( "spark.cdcloader.input.tablenames" )
    tblNames.length should be ( 2 )
    tblNames( 0 ) should be( "a" )
    tblNames( 1 ) should be( "b" )
    val colPos = props.getArrayProperty( "spark.cdcloader.control.columnpositions.a" )
    colPos.length should be ( 4 )
    colPos( 0 ) should be( "1" )
    colPos( 1 ) should be( "2" )
    colPos( 2 ) should be( "3" )
    colPos( 3 ) should be( "4" )
    props.getBooleanProperty( "spark.cdcloader.columns.metadata.name.isdeleted" ) should be (
      true.asInstanceOf[java.lang.Boolean] )
  }
}
