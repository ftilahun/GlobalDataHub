package com.kainos.enstar.globaldatahub.cdcloader.processor

import com.kainos.enstar.globaldatahub.TestContexts
import com.kainos.enstar.globaldatahub.cdcloader.control.ControlProcessor
import com.kainos.enstar.globaldatahub.cdcloader.udfs.CDCUserFunctions
import com.kainos.enstar.globaldatahub.common.io.{DataFrameReader, DataFrameTableOperations, DataFrameWriter, SQLReader}
import com.kainos.enstar.globaldatahub.common.properties.GDHProperties
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.mockito.Mockito
import org.mockito.mock.SerializableMode
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

/**
 * Unit tests for CDCTableProcessor
 */
class CDCTableProcessorSpec extends FlatSpec with GivenWhenThen with Matchers {

  "CDCTableProcessor" should "load the initial table data" in {
    Given( "A control table" )

    val tableName = "Policy"
    val cdcTableProcessor = new CDCTableProcessor
    val controlProcessor = Mockito.mock( classOf[ControlProcessor] )
    val reader = Mockito.mock( classOf[DataFrameReader] )
    val properties =
      Mockito.mock( classOf[GDHProperties],
        Mockito
          .withSettings()
          .serializable( SerializableMode.ACROSS_CLASSLOADERS ) )
    val userFunctions = new CDCUserFunctions

    When( "The control table contains 0 rows for this source" )
    Mockito
      .when( properties.getStringProperty( "spark.cdcloader.paths.data.basedir" ) )
      .thenReturn( "/path/to/initialdata" )
    Mockito
      .when(
        properties.getStringProperty(
          "spark.cdcloader.columns.metadata.name.loadtimestamp" ) )
      .thenReturn( "_timeStamp" )
    Mockito
      .when(
        properties.getStringProperty( "spark.cdcloader.format.timestamp.hive" ) )
      .thenReturn( "YYYY-MM-DD HH:mm:ss.SSS" )
    Mockito
      .when(
        properties.getStringProperty(
          "spark.cdcloader.format.timestamp.attunity" ) )
      .thenReturn( "YYYYMMDDHmmsshh" )
    Mockito
      .when(
        properties.getStringProperty(
          "spark.cdcloader.columns.attunity.name.changesequence" ) )
      .thenReturn( "lastchangesequence" )
    Mockito
      .when(
        properties.getStringProperty(
          "spark.cdcloader.columns.metadata.name.isdeleted" ) )
      .thenReturn( "isdeleted" )
    Mockito
      .when(
        properties.getStringProperty(
          "spark.cdcloader.columns.attunity.name.changeoperation" ) )
      .thenReturn( "_operation" )
    Mockito
      .when(
        properties.getBooleanProperty(
          "spark.cdcloader.control.changemask.enabled" ) )
      .thenReturn( false )
    Mockito
      .when(
        controlProcessor.isInitialLoad(
          org.mockito.Matchers.any( classOf[SQLContext] ),
          org.mockito.Matchers.anyString(),
          org.mockito.Matchers.any( classOf[GDHProperties] ) ) )
      .thenReturn( true )
    Mockito
      .when(
        reader.read( org.mockito.Matchers.any( classOf[SQLContext] ),
          org.mockito.Matchers.anyString,
          org.mockito.Matchers.any( classOf[Some[StorageLevel]] ) ) )
      .thenReturn( TestContexts.dummyData( 10 ) )

    Then( "The table processor should read the initial table data" )

    cdcTableProcessor
      .load( tableName,
        TestContexts.sqlContext,
        controlProcessor,
        reader,
        userFunctions,
        properties )
      .collect()
      .foreach { row =>
        row.getString( 3 ) should be( "INSERT" )
        row.getBoolean( 5 ) should be( false )
      }

    Mockito
      .verify( controlProcessor, Mockito.times( 1 ) )
      .isInitialLoad(
        org.mockito.Matchers.any( classOf[SQLContext] ),
        org.mockito.Matchers.anyString(),
        org.mockito.Matchers.any( classOf[GDHProperties] )
      )
    Mockito
      .verify( reader, Mockito.times( 1 ) )
      .read(
        org.mockito.Matchers.any( classOf[SQLContext] ),
        org.mockito.Matchers.anyString(),
        org.mockito.Matchers.any( classOf[Option[StorageLevel]] )
      )
    Mockito
      .verify( properties, Mockito.times( 6 ) )
      .getStringProperty( org.mockito.Matchers.anyString() )
  }

  "CDCTableProcessor" should "load the change table data" in {

    Given( "A control table" )
    val tableName = "Claims"
    val cdcTableProcessor = new CDCTableProcessor
    val controlProcessor = Mockito.mock( classOf[ControlProcessor] )
    val reader = Mockito.mock( classOf[DataFrameReader] )
    val properties =
      Mockito.mock( classOf[GDHProperties],
        Mockito
          .withSettings()
          .serializable( SerializableMode.ACROSS_CLASSLOADERS ) )
    val userFunctions = new CDCUserFunctions

    When( "The control table contains 1 or more rows for this source" )
    Mockito
      .when( properties.getStringProperty( "spark.cdcloader.paths.data.basedir" ) )
      .thenReturn( "/path/to/initialdata" )
    Mockito
      .when(
        properties.getStringProperty(
          "spark.cdcloader.columns.metadata.name.loadtimestamp" ) )
      .thenReturn( "_timeStamp" )
    Mockito
      .when(
        properties.getStringProperty( "spark.cdcloader.format.timestamp.hive" ) )
      .thenReturn( "YYYY-MM-DD HH:mm:ss.SSS" )
    Mockito
      .when(
        properties.getStringProperty(
          "spark.cdcloader.format.timestamp.attunity" ) )
      .thenReturn( "YYYYMMDDHmmsshh" )
    Mockito
      .when(
        properties.getStringProperty(
          "spark.cdcloader.columns.attunity.name.changesequence" ) )
      .thenReturn( "_changesequence" )
    Mockito
      .when(
        properties.getStringProperty(
          "spark.cdcloader.columns.metadata.name.isdeleted" ) )
      .thenReturn( "_isdeleted" )
    Mockito
      .when(
        properties.getStringProperty(
          "spark.cdcloader.columns.attunity.name.changeoperation" ) )
      .thenReturn( "_operation" )
    Mockito
      .when(
        properties.getStringProperty(
          "spark.cdcloader.columns.attunity.value.changeoperation" ) )
      .thenReturn( "DELETE" )
    Mockito
      .when(
        properties.getBooleanProperty(
          "spark.cdcloader.control.changemask.enabled" ) )
      .thenReturn( false )
    Mockito
      .when(
        controlProcessor.isInitialLoad(
          org.mockito.Matchers.any( classOf[SQLContext] ),
          org.mockito.Matchers.anyString(),
          org.mockito.Matchers.any( classOf[GDHProperties] ) ) )
      .thenReturn( false )
    Mockito
      .when(
        reader.read( org.mockito.Matchers.any( classOf[SQLContext] ),
          org.mockito.Matchers.anyString,
          org.mockito.Matchers.any( classOf[Some[StorageLevel]] ) ) )
      .thenReturn( TestContexts.changeDummyData( 10 ) )

    Then( "The table processor should read the change table data" )
    cdcTableProcessor
      .load( tableName,
        TestContexts.sqlContext,
        controlProcessor,
        reader,
        userFunctions,
        properties )
      .collect()
      .foreach { row =>
        if ( row.getString( 2 ).equalsIgnoreCase( "DELETE" ) ) {
          row.getBoolean( 6 ) should be( true )
        } else {
          row.getBoolean( 6 ) should be( false )
        }
      }

    Mockito
      .verify( controlProcessor, Mockito.times( 1 ) )
      .isInitialLoad(
        org.mockito.Matchers.any( classOf[SQLContext] ),
        org.mockito.Matchers.anyString(),
        org.mockito.Matchers.any( classOf[GDHProperties] )
      )
    Mockito
      .verify( reader, Mockito.times( 1 ) )
      .read(
        org.mockito.Matchers.any( classOf[SQLContext] ),
        org.mockito.Matchers.anyString(),
        org.mockito.Matchers.any( classOf[Option[StorageLevel]] )
      )
    Mockito
      .verify( properties, Mockito.times( 5 ) )
      .getStringProperty( org.mockito.Matchers.anyString() )
    Mockito
      .verify( controlProcessor, Mockito.times( 1 ) )
      .isInitialLoad(
        org.mockito.Matchers.any( classOf[SQLContext] ),
        org.mockito.Matchers.anyString(),
        org.mockito.Matchers.any( classOf[GDHProperties] )
      )
  }

  "CDCTableProcessor" should "Register a temp table and query the data" in {
    val tableName = "Policy"
    val query = " SELECT id, value, _changesequence, _operation, _isdeleted, " +
      "_timestamp FROM " + tableName + " WHERE _operation != 'BEFOREIMAGE' " +
      " AND _changesequence > "
    val changeSeq = "20160712111232100000000000000000000"
    val cdcTableProcessor = new CDCTableProcessor
    val controlProcessor = Mockito.mock( classOf[ControlProcessor] )
    val reader = Mockito.mock( classOf[DataFrameReader] )
    val writer = Mockito.mock( classOf[DataFrameWriter] )
    val properties =
      Mockito.mock( classOf[GDHProperties],
        Mockito
          .withSettings()
          .serializable( SerializableMode.ACROSS_CLASSLOADERS ) )
    val userFunctions = new CDCUserFunctions
    val tableOperations = Mockito.mock( classOf[DataFrameTableOperations] )
    val sqlReader = Mockito.mock( classOf[SQLReader] )
    Mockito
      .when( properties.getStringProperty( "spark.cdcloader.paths.data.basedir" ) )
      .thenReturn( "/path/to/initialdata" )
    Mockito
      .when(
        properties.getStringProperty(
          "spark.cdcloader.columns.metadata.name.loadtimestamp" ) )
      .thenReturn( "_timestamp" )
    Mockito
      .when(
        properties.getStringProperty( "spark.cdcloader.format.timestamp.hive" ) )
      .thenReturn( "YYYY-MM-DD HH:mm:ss.SSS" )
    Mockito
      .when(
        properties.getStringProperty(
          "spark.cdcloader.format.timestamp.attunity" ) )
      .thenReturn( "YYYYMMDDHmmsshh" )
    Mockito
      .when(
        properties.getStringProperty(
          "spark.cdcloader.columns.attunity.name.changesequence" ) )
      .thenReturn( "_changesequence" )
    Mockito
      .when(
        properties.getStringProperty(
          "spark.cdcloader.columns.metadata.name.isdeleted" ) )
      .thenReturn( "_isdeleted" )
    Mockito
      .when(
        properties.getStringProperty(
          "spark.cdcloader.columns.attunity.name.changeoperation" ) )
      .thenReturn( "_operation" )
    Mockito
      .when(
        properties.getStringProperty(
          "spark.cdcloader.columns.attunity.value.changeoperation" ) )
      .thenReturn( "DELETE" )
    Mockito
      .when(
        properties.getBooleanProperty(
          "spark.cdcloader.control.changemask.enabled" ) )
      .thenReturn( false )
    Mockito
      .when(
        controlProcessor.isInitialLoad(
          org.mockito.Matchers.any( classOf[SQLContext] ),
          org.mockito.Matchers.anyString(),
          org.mockito.Matchers.any( classOf[GDHProperties] ) ) )
      .thenReturn( false )
    Mockito
      .when(
        reader.read( org.mockito.Matchers.any( classOf[SQLContext] ),
          org.mockito.Matchers.anyString,
          org.mockito.Matchers.any( classOf[Some[StorageLevel]] ) ) )
      .thenReturn( TestContexts.changeDummyData( 10 ) )
    Mockito
      .when(
        sqlReader.getSQLString( org.mockito.Matchers.any( classOf[SparkContext] ),
          org.mockito.Matchers.anyString )
      )
      .thenReturn( query )
    Mockito
      .when(
        controlProcessor.getLastSequenceNumber(
          org.mockito.Matchers.any( classOf[SQLContext] ),
          org.mockito.Matchers.any( classOf[SQLReader] ),
          org.mockito.Matchers.any( classOf[GDHProperties] ),
          org.mockito.Matchers.anyString()
        ) )
      .thenReturn( changeSeq )
    Mockito
      .when(
        tableOperations.registerTempTable(
          org.mockito.Matchers.any( classOf[DataFrame] ),
          org.mockito.Matchers.anyString()
        ) )
      .thenCallRealMethod()
    Mockito
      .when(
        tableOperations.deRegisterTempTable(
          org.mockito.Matchers.any( classOf[SQLContext] ),
          org.mockito.Matchers.anyString()
        ) )
      .thenCallRealMethod()
    Mockito
      .when(
        writer.write( org.mockito.Matchers.any( classOf[SQLContext] ),
          org.mockito.Matchers.anyString(),
          org.mockito.Matchers.any( classOf[DataFrame] ),
          org.mockito.Matchers.any( classOf[Option[StorageLevel]] ) )
      )
      .thenReturn( 8 )
    Given( "A query: " + query + changeSeq )
    When( "There are 10 rows in the table" )
    Then( "The BeforeImage rows should be filtered" )

    val data = cdcTableProcessor.process( tableName,
      TestContexts.sqlContext,
      controlProcessor,
      properties,
      reader,
      userFunctions,
      tableOperations,
      sqlReader )
    data.count should be( 8 )
    data.collect().foreach { row =>
      if ( row.getString( 3 ).equalsIgnoreCase( "DELETE" ) ) {
        row.getBoolean( 4 ) should be( true )
      } else {
        row.getBoolean( 4 ) should be( false )
      }
      row.getString( 3 ) shouldNot be( "BEFOREIMAGE" )
    }
    cdcTableProcessor.save( TestContexts.sqlContext,
      writer,
      properties,
      data,
      tableName ) should be( 8 )

    Mockito
      .verify( tableOperations, Mockito.times( 1 ) )
      .registerTempTable(
        org.mockito.Matchers.any( classOf[DataFrame] ),
        org.mockito.Matchers.anyString()
      )
    Mockito
      .verify( tableOperations, Mockito.times( 1 ) )
      .deRegisterTempTable(
        org.mockito.Matchers.any( classOf[SQLContext] ),
        org.mockito.Matchers.anyString()
      )
    Mockito
      .verify( controlProcessor, Mockito.times( 1 ) )
      .isInitialLoad(
        org.mockito.Matchers.any( classOf[SQLContext] ),
        org.mockito.Matchers.anyString(),
        org.mockito.Matchers.any( classOf[GDHProperties] )
      )
    Mockito
      .verify( reader, Mockito.times( 1 ) )
      .read(
        org.mockito.Matchers.any( classOf[SQLContext] ),
        org.mockito.Matchers.anyString(),
        org.mockito.Matchers.any( classOf[Option[StorageLevel]] )
      )
    Mockito
      .verify( properties, Mockito.times( 7 ) )
      .getStringProperty( org.mockito.Matchers.anyString() )

    Mockito
      .verify( writer, Mockito.times( 1 ) )
      .write(
        org.mockito.Matchers.any( classOf[SQLContext] ),
        org.mockito.Matchers.anyString(),
        org.mockito.Matchers.any( classOf[DataFrame] ),
        org.mockito.Matchers.any( classOf[Option[StorageLevel]] )
      )
    Mockito
      .verify( controlProcessor, Mockito.times( 1 ) )
      .isInitialLoad(
        org.mockito.Matchers.any( classOf[SQLContext] ),
        org.mockito.Matchers.anyString(),
        org.mockito.Matchers.any( classOf[GDHProperties] )
      )
  }

  "CDCTableProcessor" should "Filter columns on a change mask" in {
    val tableName = "Policy"
    val query = " SELECT id, value, _changesequence, _operation, _isdeleted, " +
      "_timestamp FROM " + tableName + " WHERE _operation != 'BEFOREIMAGE' " +
      " AND _changesequence > "
    val changeSeq = "20160712111232100000000000000000000"
    val cdcTableProcessor = new CDCTableProcessor
    val controlProcessor = Mockito.mock( classOf[ControlProcessor] )
    val reader = Mockito.mock( classOf[DataFrameReader] )
    val properties =
      Mockito.mock( classOf[GDHProperties],
        Mockito
          .withSettings()
          .serializable( SerializableMode.ACROSS_CLASSLOADERS ) )
    val userFunctions = new CDCUserFunctions
    val tableOperations = Mockito.mock( classOf[DataFrameTableOperations] )
    val sqlReader = Mockito.mock( classOf[SQLReader] )
    Mockito
      .when( properties.getStringProperty( "spark.cdcloader.paths.data.basedir" ) )
      .thenReturn( "/path/to/initialdata" )
    Mockito
      .when(
        properties.getStringProperty(
          "spark.cdcloader.columns.metadata.name.loadtimestamp" ) )
      .thenReturn( "_timestamp" )
    Mockito
      .when(
        properties.getStringProperty( "spark.cdcloader.format.timestamp.hive" ) )
      .thenReturn( "YYYY-MM-DD HH:mm:ss.SSS" )
    Mockito
      .when(
        properties.getStringProperty(
          "spark.cdcloader.format.timestamp.attunity" ) )
      .thenReturn( "YYYYMMDDHmmsshh" )
    Mockito
      .when(
        properties.getStringProperty(
          "spark.cdcloader.columns.attunity.name.changesequence" ) )
      .thenReturn( "_changesequence" )
    Mockito
      .when(
        properties.getStringProperty(
          "spark.cdcloader.columns.metadata.name.isdeleted" ) )
      .thenReturn( "_isdeleted" )
    Mockito
      .when(
        properties.getStringProperty(
          "spark.cdcloader.columns.attunity.name.changeoperation" ) )
      .thenReturn( "_operation" )
    Mockito
      .when(
        properties.getStringProperty(
          "spark.cdcloader.columns.attunity.value.changeoperation" ) )
      .thenReturn( "DELETE" )
    Mockito
      .when(
        properties.getBooleanProperty(
          "spark.cdcloader.control.changemask.enabled" ) )
      .thenReturn( true )
    Mockito
      .when(
        properties.getStringProperty(
          "spark.cdcloader.columns.attunity.name.changemask" ) )
      .thenReturn( "_changemask" )
    Mockito
      .when(
        properties.getArrayProperty(
          "spark.cdcloader.control.columnpositions" + tableName ) )
      .thenReturn( Array[String]( "9", "10" ) )
    Mockito
      .when(
        controlProcessor.isInitialLoad(
          org.mockito.Matchers.any( classOf[SQLContext] ),
          org.mockito.Matchers.anyString(),
          org.mockito.Matchers.any( classOf[GDHProperties] ) ) )
      .thenReturn( false )
    Mockito
      .when(
        reader.read( org.mockito.Matchers.any( classOf[SQLContext] ),
          org.mockito.Matchers.anyString,
          org.mockito.Matchers.any( classOf[Some[StorageLevel]] ) ) )
      .thenReturn( TestContexts.changeDummyData( 10 ) )
    Mockito
      .when(
        sqlReader.getSQLString( org.mockito.Matchers.any( classOf[SparkContext] ),
          org.mockito.Matchers.anyString )
      )
      .thenReturn( query )
    Mockito
      .when(
        controlProcessor.getLastSequenceNumber(
          org.mockito.Matchers.any( classOf[SQLContext] ),
          org.mockito.Matchers.any( classOf[SQLReader] ),
          org.mockito.Matchers.any( classOf[GDHProperties] ),
          org.mockito.Matchers.anyString()
        ) )
      .thenReturn( changeSeq )
    Mockito
      .when(
        tableOperations.registerTempTable(
          org.mockito.Matchers.any( classOf[DataFrame] ),
          org.mockito.Matchers.anyString()
        ) )
      .thenCallRealMethod()
    Mockito
      .when(
        tableOperations.deRegisterTempTable(
          org.mockito.Matchers.any( classOf[SQLContext] ),
          org.mockito.Matchers.anyString()
        ) )
      .thenCallRealMethod()
    Given( "A query: " + query + changeSeq )
    When( "There are 10 rows in the table" )
    Then( "Any rows that do not match the changemask  should be filtered" )

    val data = cdcTableProcessor.process( tableName,
      TestContexts.sqlContext,
      controlProcessor,
      properties,
      reader,
      userFunctions,
      tableOperations,
      sqlReader )
    data.count should be( 2 )
    data.collect().foreach { row =>
      if ( row.getString( 3 ).equalsIgnoreCase( "DELETE" ) ) {
        row.getBoolean( 4 ) should be( true )
      } else {
        row.getBoolean( 4 ) should be( false )
      }
      row.getString( 3 ) shouldNot be( "BEFOREIMAGE" )
    }

    Mockito
      .verify( tableOperations, Mockito.times( 1 ) )
      .registerTempTable(
        org.mockito.Matchers.any( classOf[DataFrame] ),
        org.mockito.Matchers.anyString()
      )
    Mockito
      .verify( tableOperations, Mockito.times( 1 ) )
      .deRegisterTempTable(
        org.mockito.Matchers.any( classOf[SQLContext] ),
        org.mockito.Matchers.anyString()
      )
    Mockito
      .verify( properties, Mockito.times( 7 ) )
      .getStringProperty( org.mockito.Matchers.anyString() )

    Mockito
      .verify( sqlReader, Mockito.times( 1 ) )
      .getSQLString(
        org.mockito.Matchers.any( classOf[SparkContext] ),
        org.mockito.Matchers.anyString()
      )
    Mockito
      .verify( reader, Mockito.times( 1 ) )
      .read(
        org.mockito.Matchers.any( classOf[SQLContext] ),
        org.mockito.Matchers.anyString(),
        org.mockito.Matchers.any( classOf[Option[StorageLevel]] )
      )
    Mockito
      .verify( controlProcessor, Mockito.times( 1 ) )
      .isInitialLoad(
        org.mockito.Matchers.any( classOf[SQLContext] ),
        org.mockito.Matchers.anyString(),
        org.mockito.Matchers.any( classOf[GDHProperties] )
      )
  }
}
