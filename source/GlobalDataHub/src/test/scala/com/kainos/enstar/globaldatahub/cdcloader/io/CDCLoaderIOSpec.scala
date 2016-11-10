package com.kainos.enstar.globaldatahub.cdcloader.io

import com.kainos.enstar.globaldatahub.TestContexts
import com.kainos.enstar.globaldatahub.io.DataframeIO
import org.apache.hadoop.fs.{ PathExistsException, PathNotFoundException }
import org.apache.hadoop.mapred.InvalidInputException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.storage.StorageLevel
import org.mockito.Mockito
import org.scalatest.{ FlatSpec, GivenWhenThen, Matchers }

/**
  * Unit tests for CDCLoaderIO
  */
class CDCLoaderIOSpec extends FlatSpec
    with GivenWhenThen
    with Matchers {

  "CDCLoaderIO" should "Read input data" in {
    val dataframeIO = Mockito.mock( classOf[DataframeIO] )
    val cdcLoaderIO = new CDCLoaderIO( dataframeIO )
    Mockito.when( dataframeIO.read( TestContexts.sqlContext, "/some/path/", None ) ).
      thenReturn( TestContexts.dummyData( 5 ) )

    Given( "The input \"/some/path/\"" )
    val df = cdcLoaderIO.read( TestContexts.sqlContext, "/some/path/", None )
    When( "The dataset contains 5 rows" )
    Then( "Then a Dataframe should be returned with 5 rows" )
    df.count should be ( 5 )

    Given( "The input \"/some/invalid/path/\"" )
    Mockito.when( dataframeIO.read( TestContexts.sqlContext, "/some/invalid/path/", None ) ).
      thenThrow( classOf[InvalidInputException] )
    When( "The path does not exist" )
    Then( "An exception should be raised" )
    an[PathNotFoundException] should be thrownBy {
      val df = cdcLoaderIO.read( TestContexts.sqlContext, "/some/invalid/path/", None )
    }
  }

  "CDCLoaderIO" should "Write output data" in {
    val dataframeIO = Mockito.mock( classOf[DataframeIO] )
    val cdcLoaderIO = new CDCLoaderIO( dataframeIO )
    Given( "The input \"/some/path\"" )
    val data = TestContexts.dummyData( 10 )
    When( "The dataset contains 10 rows" )
    data.count should be ( 10 )
    Then( "10 rows should be persisted to disk" )
    Mockito.when( dataframeIO.write( TestContexts.sqlContext, "/some/path", data,
      Some( StorageLevel.MEMORY_ONLY ) ) ).thenReturn( true )
    val outCount = cdcLoaderIO.write( TestContexts.sqlContext, "/some/path/", data,
      StorageLevel.MEMORY_ONLY )
    outCount should be ( 10 )

    Given( "The input \"/some/existing/path\"" )
    When( "The path already exists" )
    Mockito.when( dataframeIO.write( TestContexts.sqlContext, "/some/existing/path", data,
      Some( StorageLevel.MEMORY_ONLY ) ) ).thenThrow( classOf[AnalysisException] )
    Then( "An exception should be rasied" )
    an[PathExistsException] should be thrownBy {
      cdcLoaderIO.write( TestContexts.sqlContext, "/some/existing/path", data,
        StorageLevel.MEMORY_ONLY )
    }

  }

  "CDCLoaderIO" should "Register and drop a table" in {
    val tableName = "DummyData"
    val dataframeIO = Mockito.mock( classOf[DataframeIO] )
    val cdcLoaderIO = new CDCLoaderIO( dataframeIO )
    Given( "A query on a table " )
    When( "The table has 10 rows" )
    cdcLoaderIO.registerTempTable( TestContexts.dummyData( 10 ), tableName )
    Then( "10 rows should be returned from a SQL Query" )
    TestContexts.sqlContext.sql( s"select * from $tableName" ).count should be ( 10 )

    Given( "A query on a table" )
    When( "The table does not exist" )
    Then( "An exception should be raised" )
    cdcLoaderIO.deRegisterTempTable( TestContexts.sqlContext, tableName )
    an[RuntimeException] should be thrownBy {
      TestContexts.sqlContext.sql( s"select * from $tableName" ).count should be( 10 )
    }
  }
}
