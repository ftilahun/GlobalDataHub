package com.kainos.enstar.globaldatahub.cdcloader.udfs

import com.kainos.enstar.globaldatahub.TestContexts
import com.kainos.enstar.globaldatahub.properties.GDHProperties
import org.joda.time.{ DateTime, DateTimeUtils }
import org.joda.time.format.DateTimeFormat
import org.mockito.Mockito
import org.scalatest.{ FlatSpec, GivenWhenThen, Matchers }

/**
 * Unit tests for CDCUserFunctions
 */
class CDCUserFunctionsSpec extends FlatSpec with GivenWhenThen with Matchers {

  "CDCUserFunctions" should "register UDFs" in {
    Given( "No UDFs are defined" )
    val properties = Mockito.mock( classOf[GDHProperties] )
    Mockito
      .when(
        properties.getStringProperty( "spark.cdcloader.format.timestamp.hive" ) )
      .thenReturn( "YYYY-MM-DD HH:mm:ss.SSS" )
    val userFunctions = new CDCUserFunctions
    Then( "UDfs should be registered" )
    userFunctions.registerUDFs( TestContexts.sqlContext, properties )

    Mockito
      .verify( properties, Mockito.times( 0 ) )
      .getStringProperty( org.mockito.Matchers.anyString() )
  }

  "CDCUserFunctions" should "Return the current time in hive format" in {
    Given( "S date" )
    val properties = Mockito.mock( classOf[GDHProperties] )
    When( "The format is YYYY-MM-DD HH:mm:ss.SSS" )
    Mockito
      .when(
        properties.getStringProperty( "spark.cdcloader.format.timestamp.hive" ) )
      .thenReturn( "YYYY-MM-DD HH:mm:ss.SSS" )
    val userFunctions = new CDCUserFunctions
    val date = new DateTime()
    DateTimeUtils.setCurrentMillisFixed( date.getMillis )
    Then( "The date should me formated to match" )
    userFunctions.getCurrentTime( properties ) should
      be(
        DateTimeFormat.forPattern( "YYYY-MM-DD HH:mm:ss.SSS" ).print( date )
      )
    DateTimeUtils.setCurrentMillisSystem()

    Mockito
      .verify( properties, Mockito.times( 1 ) )
      .getStringProperty( org.mockito.Matchers.anyString() )
  }

  "CDCUserFunctions" should "Check if a row is deleted" in {
    Given( "a row" )
    val properties = Mockito.mock( classOf[GDHProperties] )
    val userFunctions = new CDCUserFunctions
    When( "The filter is DELETE" )
    Mockito
      .when(
        properties.getStringProperty(
          "spark.cdcloader.columns.attunity.value.changeoperation" ) )
      .thenReturn( "DELETE" )
    Then( "DELETE should be true" )
    userFunctions.isDeleted( "DELETE", properties ) should be(
      true.asInstanceOf[java.lang.Boolean] )
    Then( "INSERT should be true" )
    userFunctions.isDeleted( "INSERT", properties ) should be(
      false.asInstanceOf[java.lang.Boolean] )

    Mockito
      .verify( properties, Mockito.times( 2 ) )
      .getStringProperty( org.mockito.Matchers.anyString() )
  }

  "CDCUserFunctions" should "covert the change mask to a bit mask correctly" in {
    val userFunctions = new CDCUserFunctions
    userFunctions.getBitMask( "0380" ) should equal( "0000000111" )
    userFunctions.getBitMask( null ) should equal( "0" )
  }

  "CDCUserFunctions" should "identify bits that have been set correctly" in {
    val userFunctions = new CDCUserFunctions
    userFunctions.isBitSet( userFunctions.getBitMask( "380" ), 9 ) should be(
      true.asInstanceOf[java.lang.Boolean] )
    userFunctions.isBitSet( userFunctions.getBitMask( "380" ), 10 ) should be(
      false.asInstanceOf[java.lang.Boolean] )
  }

  "CDCUserFunctions" should "identify all items in the change mask that have been set" in {
    val userFunctions = new CDCUserFunctions
    userFunctions.isAnyBitSet( "380", Array[String]() ) should be(
      false.asInstanceOf[java.lang.Boolean] )
    userFunctions.isAnyBitSet( "380", Array[String]( "1", "9", "10" ) ) should be(
      true.asInstanceOf[java.lang.Boolean] )
    an[NumberFormatException] should be thrownBy {
      userFunctions.isAnyBitSet( "380", Array[String]( "I", "LIKE", "TOAST" ) )
    }
  }

  "CDCUserFunctions" should "Generate a sequence number" in {
    val properties = Mockito.mock( classOf[GDHProperties] )
    val userFunctions = new CDCUserFunctions

    val date = new DateTime()
    DateTimeUtils.setCurrentMillisFixed( date.getMillis )
    Mockito
      .when(
        properties.getStringProperty(
          "spark.cdcloader.format.timestamp.attunity" ) )
      .thenReturn( "YYYYMMDDHHmmSShh" )
    userFunctions.generateSequenceNumber( properties ) should be(
      DateTimeFormat
        .forPattern( properties.getStringProperty(
          "spark.cdcloader.format.timestamp.attunity" ) )
        .print( date ) +
        "0000000000000000000"
    )
    DateTimeUtils.setCurrentMillisSystem()

    Mockito
      .verify( properties, Mockito.times( 2 ) )
      .getStringProperty( org.mockito.Matchers.anyString() )
  }
}
