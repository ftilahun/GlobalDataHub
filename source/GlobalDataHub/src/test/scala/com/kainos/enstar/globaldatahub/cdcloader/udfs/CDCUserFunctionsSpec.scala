package com.kainos.enstar.globaldatahub.cdcloader.udfs

import com.kainos.enstar.globaldatahub.TestContexts
import com.kainos.enstar.globaldatahub.properties.GDHProperties
import org.joda.time.{ DateTime, DateTimeUtils }
import org.joda.time.format.DateTimeFormat
import org.mockito.Mockito
import org.scalatest.{ FlatSpec, GivenWhenThen, Matchers }

class CDCUserFunctionsSpec extends FlatSpec with GivenWhenThen with Matchers {

  "CDCUserFunctions" should "register UDFs" in {
    Given( "No UDFs are defined" )
    val properties = Mockito.mock( classOf[GDHProperties] )
    Mockito
      .when( properties.getStringProperty( "HiveTimeStampFormat" ) )
      .thenReturn( "YYYY-MM-DD HH:mm:ss.SSS" )
    val userFunctions = new CDCUserFunctions
    Then( "UDfs should be registered" )
    userFunctions.registerUDFs( TestContexts.sqlContext, properties )
  }

  "CDCUserFunctions" should "Return the current time in hive format" in {
    Given( "S date" )
    val properties = Mockito.mock( classOf[GDHProperties] )
    When( "The format is YYYY-MM-DD HH:mm:ss.SSS" )
    Mockito
      .when( properties.getStringProperty( "HiveTimeStampFormat" ) )
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
  }

  "CDCUserFunctions" should "Check if a row is deleted" in {
    Given( "a row" )
    val properties = Mockito.mock( classOf[GDHProperties] )
    val userFunctions = new CDCUserFunctions
    When( "The filter is DELETE" )
    Mockito
      .when( properties.getStringProperty( "DeletedcolumnValue" ) )
      .thenReturn( "DELETE" )
    Then( "DELETE should be true" )
    userFunctions.isDeleted( "DELETE", properties ) should be(
      true.asInstanceOf[java.lang.Boolean] )
    Then( "INSERT should be true" )
    userFunctions.isDeleted( "INSERT", properties ) should be(
      false.asInstanceOf[java.lang.Boolean] )
  }

  "CDCUserFunctions" should "covert the change mask to a bit mask correctly" in {
    val properties = Mockito.mock( classOf[GDHProperties] )
    val userFunctions = new CDCUserFunctions
    userFunctions.getBitMask( "0380" ) should equal( "0000000111" )
    userFunctions.getBitMask( null ) should equal( "0" )
  }

  "CDCUserFunctions" should "identify bits that have been set correctly" in {
    val properties = Mockito.mock( classOf[GDHProperties] )
    val userFunctions = new CDCUserFunctions
    userFunctions.isBitSet( userFunctions.getBitMask( "380" ), 9 ) should be( true )
    userFunctions.isBitSet( userFunctions.getBitMask( "380" ), 10 ) should be(
      false )
  }

  "CDCUserFunctions" should "Generate a sequence number" in {
    val properties = Mockito.mock( classOf[GDHProperties] )
    val userFunctions = new CDCUserFunctions

    val date = new DateTime()
    DateTimeUtils.setCurrentMillisFixed( date.getMillis )
    Mockito
      .when( properties.getStringProperty( "changeSequenceTimestampFormat" ) )
      .thenReturn( "YYYYMMDDHHmmSShh" )
    userFunctions.generateSequenceNumber( properties ) should be(
      DateTimeFormat
        .forPattern(
          properties.getStringProperty( "changeSequenceTimestampFormat" ) )
        .print( date ) +
        "0000000000000000000"
    )
    DateTimeUtils.setCurrentMillisSystem()
  }
}
