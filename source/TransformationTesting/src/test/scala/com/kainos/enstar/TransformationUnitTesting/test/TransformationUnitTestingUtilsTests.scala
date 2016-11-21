package com.kainos.enstar.TransformationUnitTesting.test

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{ FlatSpec, FunSuite }
import org.mockito.{ Matchers, Mockito }
import org.mockito.Matchers.any
import com.kainos.enstar.TransformationUnitTesting.TransformationUnitTestingUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Row, SQLContext }
import org.apache.spark.sql.types.{ StringType, StructField, StructType }

/**
 * Created by terences on 21/11/2016.
 */
class TransformationUnitTestingUtilsTests extends FunSuite with DataFrameSuiteBase {

  test( "PopulateDataFrame should populate a dataframe" ) {

    // Ararnge
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
    Mockito.when( utils.populateDataFrameFromFile( any[String], any[String], any[( Array[String] ) => Row](), any[SQLContext] ) ).thenCallRealMethod()

    // Act
    val dataFrame = utils.populateDataFrameFromFile( "location", "avroLocation", col => Row( col( 0 ), col( 1 ), col( 2 ) ), sqlContext )

    // Assert
    assertDataFrameEquals( expectedDataFrame, dataFrame )
  }
}
