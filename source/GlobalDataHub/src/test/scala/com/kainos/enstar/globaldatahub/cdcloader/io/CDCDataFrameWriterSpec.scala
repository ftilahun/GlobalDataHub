package com.kainos.enstar.globaldatahub.cdcloader.io

import com.kainos.enstar.globaldatahub.TestContexts
import com.kainos.enstar.globaldatahub.io.GDHDataFrameWriter
import org.apache.hadoop.fs.PathExistsException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.storage.StorageLevel
import org.mockito.Mockito
import org.scalatest.{ FlatSpec, GivenWhenThen, Matchers }

class CDCDataFrameWriterSpec
    extends FlatSpec
    with GivenWhenThen
    with Matchers {

  "CDCDataFrameWriter" should "Write output data" in {
    val dataFrameWriter = Mockito.mock( classOf[GDHDataFrameWriter] )
    val cdcDataFrameWriter = new CDCDataFrameWriter( dataFrameWriter )
    Given( "The input \"/some/path\"" )
    val data = TestContexts.dummyData( 10 )
    When( "The dataset contains 10 rows" )
    data.count should be( 10 )
    Then( "10 rows should be persisted to disk" )
    Mockito
      .when(
        dataFrameWriter.write( TestContexts.sqlContext,
          "/some/path",
          data,
          Some( StorageLevel.MEMORY_ONLY ) ) )
      .thenReturn( true )
    val outCount = cdcDataFrameWriter.write( TestContexts.sqlContext,
      "/some/path/",
      data,
      StorageLevel.MEMORY_ONLY )
    outCount should be( 10 )

    Given( "The input \"/some/existing/path\"" )
    When( "The path already exists" )
    Mockito
      .when(
        dataFrameWriter.write( TestContexts.sqlContext,
          "/some/existing/path",
          data,
          Some( StorageLevel.MEMORY_ONLY ) ) )
      .thenThrow( classOf[AnalysisException] )
    Then( "An exception should be rasied" )
    an[PathExistsException] should be thrownBy {
      cdcDataFrameWriter.write( TestContexts.sqlContext,
        "/some/existing/path",
        data,
        StorageLevel.MEMORY_ONLY )
    }

  }

}
