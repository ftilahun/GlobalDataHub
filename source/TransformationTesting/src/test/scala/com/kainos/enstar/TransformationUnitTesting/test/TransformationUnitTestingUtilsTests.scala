package com.kainos.enstar.TransformationUnitTesting.test

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.TransformationUnitTestingUtils
import org.apache.spark.sql.types.{ StringType, StructField, StructType }
import org.apache.spark.sql.{ Row, SQLContext }
import org.mockito.Matchers.any
import org.mockito.Mockito
import org.scalatest.FunSuite

/**
 * Created by terences on 21/11/2016.
 */
class TransformationUnitTestingUtilsTests extends FunSuite with DataFrameSuiteBase {

  test( "PopulateDataFrame should populate a dataframe" ) {

    // Arrange
    val data = ( "a,b,c" :: "d,e,f" :: Nil )
    val dataRDD = sqlContext.sparkContext.parallelize( data )

    val schema = StructType(
      StructField( "field1", StringType, false ) ::
        StructField( "field2", StringType, false ) ::
        StructField( "field3", StringType, false ) :: Nil
    )

    val expectedDataFrame = sqlContext.createDataFrame( dataRDD.map( _.split( "," ) ).map( col => Row( col( 0 ), col( 1 ), col( 2 ) ) ), schema )

    val utils = Mockito.mock( classOf[TransformationUnitTestingUtils] )
    Mockito.when( utils.loadRDDFromFile( "location", sqlContext ) ).thenReturn( dataRDD )
    Mockito.when( utils.loadSchemaFromFile( "avroLocation", sqlContext ) ).thenReturn( schema )
    Mockito.when( utils.populateDataFrameFromFile( any[String], any[String], any[( String => Array[String] )](), any[( Array[String] ) => Row](), any[SQLContext] ) ).thenCallRealMethod()

    // Act
    val dataFrame = utils.populateDataFrameFromFile( "location", "avroLocation", _.split( "," ), col => Row( col( 0 ), col( 1 ), col( 2 ) ), sqlContext )

    // Assert
    assertDataFrameEquals( expectedDataFrame, dataFrame )
  }
}
