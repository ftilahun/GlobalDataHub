package com.kainos.enstar.test.TransformationUnitTesting.Deduction

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{DeductionUtils, SQLRunner, TransformationUnitTestingUtils}
import org.apache.spark.sql._
import org.scalatest.FunSuite
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

/**
 * Created by terences on 08/12/2016.
 */
class TransformationTests extends FunSuite with DataFrameSuiteBase {

  private val utils = new TransformationUnitTestingUtils
  private val testDataInputPath = "/deduction/input/"
  private val testDataOutputPath = "/deduction/output/"
  private val schemasPath = "/deduction/schemas/"

  def populateDataFrameWithLineTestData( dataFile : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( testDataInputPath + dataFile ).toString,
      getClass.getResource( schemasPath + "line.avro" ).toString,
      _.split( "," ),
      DeductionUtils.lineMapping,
      sqlc
    )
  }

  def populateDataFrameWithLayerTestData( dataFile : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( testDataInputPath + dataFile ).toString,
      getClass.getResource( schemasPath + "layer.avro" ).toString,
      _.split( "," ),
      DeductionUtils.layerMapping,
      sqlc
    )
  }

  def populateDataFrameWithLayerDeductionTestData( dataFile : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( testDataInputPath + dataFile ).toString,
      getClass.getResource( schemasPath + "layer_deduction.avro" ).toString,
      _.split( "," ),
      DeductionUtils.layerDeductionMapping,
      sqlc
    )
  }

  def populateDataFrameWithDeductionTestData( dataFile : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( testDataOutputPath + dataFile ).toString,
      getClass.getResource( schemasPath + "deduction.avro" ).toString,
      _.split( "," ),
      DeductionUtils.deductionMapping,
      sqlc
    )
  }

  test( "Deduction transformation mapping test with primary data sets" ){

    // Arrange
    val sqlc = sqlContext
    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframes
    val line = populateDataFrameWithLineTestData( "line_PrimaryTestData.csv", sqlc )
    val layer = populateDataFrameWithLayerTestData( "layer_PrimaryTestData.csv", sqlc )
    val layerDeduction = populateDataFrameWithLayerDeductionTestData( "layer_deduction_PrimaryTestData.csv", sqlc )

    val expectedDeduction = populateDataFrameWithDeductionTestData( "deduction_PrimaryTestData.csv", sqlc )

    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    layerDeduction.registerTempTable( "layer_deduction" )

    // Load the hql statement under test
    val tmptableStatement = utils.loadHQLStatementFromResource("Transformation/DeductionPre.hql")
    val statement = utils.loadHQLStatementFromResource( "Transformation/Deduction.hql" )

    // Act
    val tmptable = SQLRunner.runStatement(tmptableStatement, sqlc).registerTempTable("tmptable")
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert
    assertDataFrameEquals( expectedDeduction, result )
  }

  test( "Deduction transformation mapping test calculating CalculatedDeductionAmount for policy with single deduction" ){

    // Arrange
    val sqlc = sqlContext
    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframes
    val line = populateDataFrameWithLineTestData( "line_SingleDeductionCalculatedDeductionAmount.csv", sqlc )
    val layer = populateDataFrameWithLayerTestData( "layer_SingleDeductionCalculatedDeductionAmount.csv", sqlc )
    val layerDeduction = populateDataFrameWithLayerDeductionTestData( "layer_deduction_SingleDeductionCalculatedDeductionAmount.csv", sqlc )

    val expectedDeduction = populateDataFrameWithDeductionTestData( "deduction_SingleDeductionCalculatedDeductionAmount.csv", sqlc )

    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    layerDeduction.registerTempTable( "layer_deduction" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/Deduction.hql" )

    // Act
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert
    assertDataFrameEquals( expectedDeduction, result )
  }

  test("Deduction transformation mapping test multiple deductions on a line, monotonic sequence no") {

    // Arrange
    val sqlc = sqlContext
    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframes
    val line = populateDataFrameWithLineTestData( "line_MultipleDeductionMonotonicSeq.csv", sqlc )
    val layer = populateDataFrameWithLayerTestData( "layer_MultipleDeductionMonotonicSeq.csv", sqlc )
    val layerDeduction = populateDataFrameWithLayerDeductionTestData( "layer_deduction_MultipleDeductionMonotonicSeq.csv", sqlc )

    val expectedDeduction = populateDataFrameWithDeductionTestData( "deduction_MultipleDeductionMonotonicSeq.csv", sqlc )

    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    layerDeduction.registerTempTable( "layer_deduction" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/Deduction.hql" )

    // Act
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert
    assertDataFrameEquals( expectedDeduction, result )

  }

  test("Deduction transformation mapping test, multiple lines each with multiple deductions") {

    // Arrange
    val sqlc = sqlContext
    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframes
    val line = populateDataFrameWithLineTestData( "line_MulitpleDeductionMonotonicSeqMultipleLines.csv", sqlc )
    val layer = populateDataFrameWithLayerTestData( "layer_MulitpleDeductionMonotonicSeqMultipleLines.csv", sqlc )
    val layerDeduction = populateDataFrameWithLayerDeductionTestData( "layer_deduction_MulitpleDeductionMonotonicSeqMultipleLines.csv", sqlc )

    val expectedDeduction = populateDataFrameWithDeductionTestData( "deduction_MulitpleDeductionMonotonicSeqMultipleLines.csv", sqlc )

    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    layerDeduction.registerTempTable( "layer_deduction" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/Deduction.hql" )

    // Act
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert
    assertDataFrameEquals( expectedDeduction, result )

  }
}
