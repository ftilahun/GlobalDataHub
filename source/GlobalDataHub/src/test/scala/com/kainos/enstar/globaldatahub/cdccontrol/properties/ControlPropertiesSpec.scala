package com.kainos.enstar.globaldatahub.cdccontrol.properties

import com.kainos.enstar.globaldatahub.common.exceptions.PropertyNotSetException
import org.scalatest.{ FlatSpec, GivenWhenThen, Matchers }

/**
 * Unit tests for ControlProperties
 */
class ControlPropertiesSpec
    extends FlatSpec
    with GivenWhenThen
    with Matchers {

  "ControlProperties" should "Parse the command line arguments" in {
    Given( "A set of command line args" )
    val argsArray = Array[String](
      "--ctrlOptions",
      "spark.cdccontrol.path.sql=a," +
        "spark.cdccontrol.path.data.control.input=b," +
        "spark.cdccontrol.path.data.control.output=c," +
        "spark.cdccontrol.path.data.input=d," +
        "spark.cdccontrol.tables.control.name=e," +
        "spark.cdccontrol.tables.temp.name=f"
    )
    When( "Parsing arguments" )
    val props = ControlProperties.parseProperties( argsArray )

    Then( "Property values should be set " )
    props( "spark.cdccontrol.path.sql" ) should be ( "a" )
    props( "spark.cdccontrol.path.data.control.input" ) should be ( "b" )
    props( "spark.cdccontrol.path.data.control.output" ) should be ( "c" )
    props( "spark.cdccontrol.path.data.input" ) should be ( "d" )
    props( "spark.cdccontrol.tables.control.name" ) should be ( "e" )
    props( "spark.cdccontrol.tables.temp.name" ) should be ( "f" )
  }

  "ControlProperties" should "Throw when unable to parse command line options" in {
    Given( "Invalid input" )
    When( "--cdcOptions has no arguments" )
    Then( "a PropertyNotSetException should be thrown" )
    an[PropertyNotSetException] should be thrownBy {
      ControlProperties.parseProperties( Array[String]( "--ctrlOptions" ) )
    }
    Given( "Invalid input" )
    When( "--cdcOptions was not set" )
    Then( "a PropertyNotSetException should be thrown" )
    an[PropertyNotSetException] should be thrownBy {
      ControlProperties.parseProperties( Array[String]() )
    }
    Given( "Invalid input" )
    When( "Passed a null reference" )
    Then( "a NullPointerException should be thrown" )
    an[NullPointerException] should be thrownBy {
      ControlProperties.parseProperties( null )
    }
  }

  "ControlProperties" should "Check properties have been set" in {
    Given( "Valid input" )
    When( "Any individual property has not been set" )
    Then( "a PropertyNotSetException should be thrown" )
    an[PropertyNotSetException] should be thrownBy {
      val argsArray = Array[String](
        "--ctrlOptions",
        "spark.cdccontrol.path.sql=a," +
          "spark.cdccontrol.path.data.control.input=b," +
          "spark.cdccontrol.path.data.control.output=c," +
          "spark.cdccontrol.tables.control.name=e," +
          "spark.cdccontrol.tables.temp.name=f"
      )
      new ControlProperties( ControlProperties.parseProperties( argsArray ) )
    }

    Given( "Valid input" )
    When( "All propertie have been set correctly" )
    Then( "all properties should have been set" )

    val argsArray = Array[String](
      "--ctrlOptions",
      "spark.cdccontrol.path.sql=a," +
        "spark.cdccontrol.path.data.control.input=b," +
        "spark.cdccontrol.path.data.control.output=c," +
        "spark.cdccontrol.path.data.input=d," +
        "spark.cdccontrol.tables.control.name=e," +
        "spark.cdccontrol.tables.temp.name=f"
    )
    new ControlProperties( ControlProperties.parseProperties( argsArray ) )
  }
}
