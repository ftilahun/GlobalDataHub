package com.kainos.enstar.globaldatahub.cdcloader.control

import com.kainos.enstar.globaldatahub.TestContexts
import com.kainos.enstar.globaldatahub.cdcloader.io.{
  CDCTableOperations,
  DataFrameReader,
  SQLFileReader
}
import com.kainos.enstar.globaldatahub.properties.GDHProperties
import org.apache.spark.sql.DataFrame
import org.joda.time.format.DateTimeFormat
import org.joda.time.{ DateTime, DateTimeUtils }
import org.mockito.Mockito
import org.scalatest.{ FlatSpec, GivenWhenThen, Matchers }

/**
 * Unit tests for the control processor
 */
class ControlProcessorSpec extends FlatSpec with GivenWhenThen with Matchers {

  "Controlprocessor" should "Register the control table" in {
    val reader : DataFrameReader = Mockito.mock( classOf[DataFrameReader] )
    val sqlReaderIO : SQLFileReader = Mockito.mock( classOf[SQLFileReader] )
    val properties : GDHProperties = Mockito.mock( classOf[GDHProperties] )
    val tableOperations : CDCTableOperations =
      Mockito.mock( classOf[CDCTableOperations] )
    val controlProcessor : CDCControlProcessor =
      new CDCControlProcessor( TestContexts.sqlContext,
        reader,
        sqlReaderIO,
        properties,
        tableOperations )

    Given( "The input /control/dir/" )
    Mockito
      .when( properties.getStringProperty( "controlTablePath" ) )
      .thenReturn( "/control/dir/" )
    Mockito
      .when( properties.getStringProperty( "controlTableName" ) )
      .thenReturn( "control" )
    When( "The input is valid" )
    Mockito
      .when(
        reader.read( TestContexts.sqlContext,
          properties.getStringProperty( "controlTablePath" ),
          None ) )
      .thenReturn( TestContexts.generateControlTable( 10 ) )
    Mockito
      .when(
        tableOperations.registerTempTable(
          org.mockito.Matchers.any( classOf[DataFrame] ),
          org.mockito.Matchers.anyString() ) )
      .thenCallRealMethod()
    Then( "The control table should be created" )
    controlProcessor.registerControlTable()
    TestContexts.sqlContext
      .sql( "SELECT * FROM " + properties.getStringProperty( "controlTableName" ) )
      .count should be( 10 )

    Given( "The control table has been created" )
    When( "The control table is de-registered" )
    Mockito
      .when(
        tableOperations.deRegisterTempTable(
          TestContexts.sqlContext,
          properties.getStringProperty( "controlTableName" ) ) )
      .thenCallRealMethod()
    controlProcessor.deregisterControlTable()
    Then( "an exception should be thrown" )
    an[RuntimeException] should be thrownBy {
      TestContexts.sqlContext.sql(
        "SELECT * FROM " + properties.getStringProperty( "controlTableName" ) )
    }
  }

  "Controlprocessor" should "Retrieve the last sequence processed" in {
    val reader : DataFrameReader = Mockito.mock( classOf[DataFrameReader] )
    val sqlReaderIO : SQLFileReader = Mockito.mock( classOf[SQLFileReader] )
    val properties : GDHProperties = Mockito.mock( classOf[GDHProperties] )
    val tableOperations : CDCTableOperations =
      Mockito.mock( classOf[CDCTableOperations] )
    val controlProcessor : CDCControlProcessor =
      new CDCControlProcessor( TestContexts.sqlContext,
        reader,
        sqlReaderIO,
        properties,
        tableOperations )

    val date = new DateTime()
    DateTimeUtils.setCurrentMillisFixed( date.getMillis )

    Given( "A populated control table" )
    Mockito
      .when( properties.getStringProperty( "controlTablePath" ) )
      .thenReturn( "/control/dir/" )
    Mockito
      .when( properties.getStringProperty( "controlTableName" ) )
      .thenReturn( "control" )
    Mockito
      .when( properties.getStringProperty( "changeSequenceTimestampFormat" ) )
      .thenReturn( "YYYYMMDDHHmmSShh" )
    Mockito
      .when(
        reader.read( TestContexts.sqlContext,
          properties.getStringProperty( "controlTablePath" ),
          None ) )
      .thenReturn( TestContexts.generateControlTable( 10 ) )
    Mockito
      .when(
        tableOperations.registerTempTable(
          org.mockito.Matchers.any( classOf[DataFrame] ),
          org.mockito.Matchers.anyString() ) )
      .thenCallRealMethod()
    controlProcessor.registerControlTable()
    When( "The control table has 10 rows" )
    Mockito
      .when( properties.getStringProperty( "controlTableSQLPath" ) )
      .thenReturn( "/some/path" )
    Mockito
      .when(
        sqlReaderIO.getSQLString(
          TestContexts.sparkContext,
          properties.getStringProperty( "controlTableSQLPath" ) ) )
      .thenReturn(
        "SELECT MAX(lastattunitychangeseq) FROM control WHERE attunitytablename = " )
    Then(
      "The ControlProcessor should return the seqence number of the 10th row" )
    controlProcessor.getLastSequenceNumber( "'policy'" ) should be(
      DateTimeFormat
        .forPattern(
          properties.getStringProperty( "changeSequenceTimestampFormat" ) )
        .print( date ) +
        "0000000000000000010"
    )
    DateTimeUtils.setCurrentMillisSystem()
  }

  "Controlprocessor" should "Generate a sequence number" in {
    val reader : DataFrameReader = Mockito.mock( classOf[DataFrameReader] )
    val sqlReaderIO : SQLFileReader = Mockito.mock( classOf[SQLFileReader] )
    val properties : GDHProperties = Mockito.mock( classOf[GDHProperties] )
    val tableOperations : CDCTableOperations =
      Mockito.mock( classOf[CDCTableOperations] )
    val controlProcessor : CDCControlProcessor =
      new CDCControlProcessor( TestContexts.sqlContext,
        reader,
        sqlReaderIO,
        properties,
        tableOperations )

    val date = new DateTime()
    Given( "No inputs" )
    DateTimeUtils.setCurrentMillisFixed( date.getMillis )
    When( "Required" )
    Then( "The control processor should generate a sequence number" )
    Mockito
      .when( properties.getStringProperty( "changeSequenceTimestampFormat" ) )
      .thenReturn( "YYYYMMDDHHmmSShh" )
    controlProcessor.generateFirstSequenceNumber should be(
      DateTimeFormat
        .forPattern(
          properties.getStringProperty( "changeSequenceTimestampFormat" ) )
        .print( date ) +
        "0000000000000000000"
    )
    DateTimeUtils.setCurrentMillisSystem()
  }

  "ControlProcessor" should "Identify whether a table is being loaded for the first time" in {
    val reader : DataFrameReader = Mockito.mock( classOf[DataFrameReader] )
    val sqlReaderIO : SQLFileReader = Mockito.mock( classOf[SQLFileReader] )
    val properties : GDHProperties = Mockito.mock( classOf[GDHProperties] )
    val tableOperations : CDCTableOperations =
      Mockito.mock( classOf[CDCTableOperations] )
    val controlProcessor : CDCControlProcessor =
      new CDCControlProcessor( TestContexts.sqlContext,
        reader,
        sqlReaderIO,
        properties,
        tableOperations )

    Given( "A control table" )
    Mockito
      .when( properties.getStringProperty( "controlTablePath" ) )
      .thenReturn( "/control/dir/" )
    Mockito
      .when( properties.getStringProperty( "controlTableName" ) )
      .thenReturn( "control" )
    Mockito
      .when( properties.getStringProperty( "attunitytablenameColumn" ) )
      .thenReturn( "attunitytablename" )
    Mockito
      .when(
        reader.read( TestContexts.sqlContext,
          properties.getStringProperty( "controlTablePath" ),
          None ) )
      .thenReturn( TestContexts.generateControlTable( 10 ) )
    Mockito
      .when(
        tableOperations.registerTempTable(
          org.mockito.Matchers.any( classOf[DataFrame] ),
          org.mockito.Matchers.anyString() ) )
      .thenCallRealMethod()
    controlProcessor.registerControlTable()
    When( "The control table has rows for a source table" )
    Then( "isInitialLoad should be false" )
    controlProcessor
      .isInitialLoad( TestContexts.sqlContext, "'policy'" ) should be( false )

    When( "The control table has no rows for a source table" )
    Then( "isInitialLoad should be true" )
    controlProcessor
      .isInitialLoad( TestContexts.sqlContext, "'transaction'" ) should be( true )

    controlProcessor.deregisterControlTable()
  }
}
