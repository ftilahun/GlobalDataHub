package com.kainos.enstar.TransformationUnitTesting.test

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.SQLRunner
import org.apache.spark.sql.{ AnalysisException, Row }
import org.apache.spark.sql.types.{ StringType, StructField, StructType }
import org.scalatest.FunSuite

/**
 * Created by terences on 21/11/2016.
 */
class SQLRunnerTests extends FunSuite with DataFrameSuiteBase {

  test( "RunStatement should run statement and return correct result for simple query" ) {

    // Arrange
    val statement = "SELECT field1 from Table"

    val inputData = ( "a,b,c" :: "d,e,f" :: Nil )
    val inputDataRDD = sqlContext.sparkContext.parallelize( inputData )

    val inputSchema = StructType(
      StructField( "field1", StringType, false ) ::
        StructField( "field2", StringType, false ) ::
        StructField( "field3", StringType, false ) :: Nil
    )

    val inputDataFrame = sqlContext.createDataFrame( inputDataRDD.map( _.split( "," ) ).map( col => Row( col( 0 ), col( 1 ), col( 2 ) ) ), inputSchema )

    val expectedData = ( "a" :: "d" :: Nil )
    val expectedDataRDD = sqlContext.sparkContext.parallelize( expectedData )
    val expectedSchema = StructType(
      StructField( "field1", StringType, false ) :: Nil
    )
    val expectedDataFrame = sqlContext.createDataFrame( expectedDataRDD.map( col => Row( col ) ), expectedSchema )

    inputDataFrame.registerTempTable( "Table" )

    // Act
    val result = SQLRunner.runStatement( statement, sqlContext )

    // Assert
    assertDataFrameEquals( expectedDataFrame, result )
  }
}
