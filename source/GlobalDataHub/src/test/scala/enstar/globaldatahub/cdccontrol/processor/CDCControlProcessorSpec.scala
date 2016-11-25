package enstar.globaldatahub.cdccontrol.processor

import enstar.globaldatahub.cdccontrol.udfs.UserFunctions
import enstar.globaldatahub.common.io.{ DataFrameReader, DataFrameWriter, SQLFileReader, TableOperations }
import enstar.globaldatahub.common.properties.GDHProperties
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.storage.StorageLevel
import org.mockito.Mockito
import org.scalatest.{ FlatSpec, GivenWhenThen, Matchers }

/**
 * Unit tests for CDCControlProcessor
 */
class CDCControlProcessorSpec
    extends FlatSpec
    with GivenWhenThen
    with Matchers {

  "CDCControlProcessor" should "register the control table" in {

    val sqlContext = Mockito.mock( classOf[SQLContext] )
    val reader = Mockito.mock( classOf[DataFrameReader] )
    val properties = Mockito.mock( classOf[GDHProperties] )
    val tableOperations = Mockito.mock( classOf[TableOperations] )

    val processor = new CDCContolProcessor

    processor.registerControlTable( sqlContext, reader, properties, tableOperations )
  }

  "CDCControlProcessor" should "de-register the control table" in {

    val sqlContext = Mockito.mock( classOf[SQLContext] )
    val properties = Mockito.mock( classOf[GDHProperties] )
    val tableOperations = Mockito.mock( classOf[TableOperations] )

    val processor = new CDCContolProcessor

    processor.deregisterControlTable( sqlContext, properties, tableOperations )
  }

  "CDCControlProcessor" should "process a dataframe" in {
    val sqlContext = Mockito.mock( classOf[SQLContext] )
    val properties = Mockito.mock( classOf[GDHProperties] )
    val tableOperations = Mockito.mock( classOf[TableOperations] )
    val reader = Mockito.mock( classOf[DataFrameReader] )
    val writer = Mockito.mock( classOf[DataFrameWriter] )
    val data = Mockito.mock( classOf[DataFrame] )
    val sqlFileReader = Mockito.mock( classOf[SQLFileReader] )
    val userFunctions = Mockito.mock( classOf[UserFunctions] )

    Given( "A control processor" )
    val processor = new CDCContolProcessor

    Mockito.when( properties.getStringProperty( "spark.cdccontrol.path.data.input" ) ).
      thenReturn( "/some/path" )
    Mockito.when( properties.getStringProperty( "spark.cdccontrol.paths.data.control.input" ) ).
      thenReturn( "/some/path" )
    Mockito.when( properties.getStringProperty( "spark.cdccontrol.paths.data.control.output" ) ).
      thenReturn( "/some/path" )

    When( "Processing a control table" )
    processor.process( sqlContext, reader, writer, sqlFileReader, tableOperations, properties, userFunctions )

    Then( "The processor should read all required properties" )
    Mockito.verify( properties, Mockito.times( 7 ) ).
      getStringProperty( org.mockito.Matchers.anyString() )

    Then( "A SQL statement for processing the data should be read." )
    Mockito.verify( sqlFileReader, Mockito.times( 1 ) ).
      getSQLString( org.mockito.Matchers.any( classOf[SparkContext] ),
        org.mockito.Matchers.anyString() )

    Then( "The data and control data should be read" )
    Mockito.verify( reader, Mockito.times( 2 ) ).
      read( org.mockito.Matchers.any( classOf[SQLContext] ),
        org.mockito.Matchers.anyString(),
        org.mockito.Matchers.any( classOf[Option[StorageLevel]] ) )

    Then( "The proessor should register temp tables for the data and the control data" )
    Mockito.verify( tableOperations, Mockito.times( 2 ) ).
      registerTempTable(
        org.mockito.Matchers.any( classOf[DataFrame] ),
        org.mockito.Matchers.anyString() )

    Then( "The processor should de-register the temp tables it has created" )
    Mockito.verify( tableOperations, Mockito.times( 2 ) ).
      deRegisterTempTable( org.mockito.Matchers.any( classOf[SQLContext] ),
        org.mockito.Matchers.anyString() )

    Then( "UDFs should be registered" )
    Mockito.verify( userFunctions, Mockito.times( 1 ) ).
      registerUDFs( org.mockito.Matchers.any( classOf[SQLContext] ),
        org.mockito.Matchers.any( classOf[GDHProperties] ) )

    Then( "The output control data should be written" )
    Mockito.verify( writer, Mockito.times( 1 ) ).
      write( org.mockito.Matchers.any( classOf[SQLContext] ),
        org.mockito.Matchers.anyString(),
        org.mockito.Matchers.any( classOf[DataFrame] ),
        org.mockito.Matchers.any( classOf[Option[StorageLevel]] )
      )
  }
}
