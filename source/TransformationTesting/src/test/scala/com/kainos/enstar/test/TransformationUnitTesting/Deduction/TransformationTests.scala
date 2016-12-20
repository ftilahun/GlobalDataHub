package com.kainos.enstar.test.TransformationUnitTesting.Deduction

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ DeductionUtils, NetAsPctOfGross, SQLRunner, TransformationUnitTestingUtils }
import org.apache.spark.sql._
import org.scalatest.FunSuite

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
    implicit val sqlc = sqlContext
    sqlContext.sparkContext.setLogLevel( "WARN" )
    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframes
    val line = utils.populateDataFrameFromCsvWithHeader("/deduction/input/line_PrimaryTestData.csv")
    val layer = utils.populateDataFrameFromCsvWithHeader("/deduction/input/layer_PrimaryTestData.csv")
    val layerDeduction = utils.populateDataFrameFromCsvWithHeader("/deduction/input/layer_deduction_PrimaryTestData.csv")

    val expectedDeduction = utils.populateDataFrameFromCsvWithHeader("/deduction/output/deduction_PrimaryTestData.csv")

    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    layerDeduction.registerTempTable( "layer_deduction" )

    sqlc.udf.register( "net_as_pct_of_gross", NetAsPctOfGross )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/Deduction.hql" )

    // Act
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

    sqlc.udf.register( "net_as_pct_of_gross", NetAsPctOfGross )

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

  test( "Deduction transformation mapping test multiple deductions on a line, monotonic sequence no" ) {

    // Arrange
    val sqlc = sqlContext
    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframes
    val line = populateDataFrameWithLineTestData( "line_PrimaryTestData.csv", sqlc )
    val layer = populateDataFrameWithLayerTestData( "layer_PrimaryTestData.csv", sqlc )
    val layerDeduction = populateDataFrameWithLayerDeductionTestData( "layer_deduction_MultipleDeductionMonotonicSeq.csv", sqlc )

    val expectedDeduction = populateDataFrameWithDeductionTestData( "deduction_MultipleDeductionMonotonicSeq.csv", sqlc )

    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    layerDeduction.registerTempTable( "layer_deduction" )

    sqlc.udf.register( "net_as_pct_of_gross", NetAsPctOfGross )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/Deduction.hql" )

    // Act
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert
    assertDataFrameEquals( expectedDeduction, result )

  }

  test( "Deduction transformation mapping test, multiple lines each with multiple deductions" ) {

    // Arrange
    val sqlc = sqlContext
    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframes
    val line = populateDataFrameWithLineTestData( "line_MultipleLines.csv", sqlc )
    val layer = populateDataFrameWithLayerTestData( "layer_MultipleLines.csv", sqlc )
    val layerDeduction = populateDataFrameWithLayerDeductionTestData( "layer_deduction_MultipleDeductionMonotonicSeqMultipleLines.csv", sqlc )

    val expectedDeduction = populateDataFrameWithDeductionTestData( "deduction_MultipleDeductionMonotonicSeqMultipleLines.csv", sqlc )

    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    layerDeduction.registerTempTable( "layer_deduction" )

    sqlc.udf.register( "net_as_pct_of_gross", NetAsPctOfGross )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/Deduction.hql" )

    // Act
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert
    assertDataFrameEquals( expectedDeduction, result )

  }

  test( "Multiple deductions all with the same sequence" ){

    // Arrange
    val sqlc = sqlContext
    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframes
    val line = populateDataFrameWithLineTestData( "line_PrimaryTestData.csv", sqlc )
    val layer = populateDataFrameWithLayerTestData( "layer_PrimaryTestData.csv", sqlc )
    val layerDeduction = populateDataFrameWithLayerDeductionTestData( "layer_deduction_MultipleDeductionAllSameSeqNo.csv", sqlc )

    val expectedDeduction = populateDataFrameWithDeductionTestData( "deduction_MultipleDeductionAllSameSeqNo.csv", sqlc )

    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    layerDeduction.registerTempTable( "layer_deduction" )

    sqlc.udf.register( "net_as_pct_of_gross", NetAsPctOfGross )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/Deduction.hql" )

    // Act
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert
    assertDataFrameEquals( expectedDeduction, result )

  }

  test( "Multiple deductions on a line not monotonic sequence" ){

    // Arrange
    val sqlc = sqlContext
    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframes
    val line = populateDataFrameWithLineTestData( "line_PrimaryTestData.csv", sqlc )
    val layer = populateDataFrameWithLayerTestData( "layer_PrimaryTestData.csv", sqlc )
    val layerDeduction = populateDataFrameWithLayerDeductionTestData( "layer_deduction_MultipleDeductionNonMonotonicSeq.csv", sqlc )

    val expectedDeduction = populateDataFrameWithDeductionTestData( "deduction_MultipleDeductionNonMonotonicSeq.csv", sqlc )

    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    layerDeduction.registerTempTable( "layer_deduction" )

    sqlc.udf.register( "net_as_pct_of_gross", NetAsPctOfGross )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/Deduction.hql" )

    // Act
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert
    assertDataFrameEquals( expectedDeduction, result )

  }

  test( "Multiple deduction on a line not monotonic sequence and out of order" ){

    // Arrange
    val sqlc = sqlContext
    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframes
    val line = populateDataFrameWithLineTestData( "line_PrimaryTestData.csv", sqlc )
    val layer = populateDataFrameWithLayerTestData( "layer_PrimaryTestData.csv", sqlc )
    val layerDeduction = populateDataFrameWithLayerDeductionTestData( "layer_deduction_MultipleDeductionNonMonotonicSeqOutOfOrder.csv", sqlc )

    val expectedDeduction = populateDataFrameWithDeductionTestData( "deduction_MultipleDeductionNonMonotonicSeqOutOfOrder.csv", sqlc )

    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    layerDeduction.registerTempTable( "layer_deduction" )

    sqlc.udf.register( "net_as_pct_of_gross", NetAsPctOfGross )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/Deduction.hql" )

    // Act
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert
    assertDataFrameEquals( expectedDeduction.orderBy( "deductionsequence" ), result.orderBy( "deductionsequence" ) )

  }

  test( "Multiple deductions on a line not monotonic sequence with multiple of same sequence no" ) {

    // Arrange
    val sqlc = sqlContext
    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframes
    val line = populateDataFrameWithLineTestData( "line_PrimaryTestData.csv", sqlc )
    val layer = populateDataFrameWithLayerTestData( "layer_PrimaryTestData.csv", sqlc )
    val layerDeduction = populateDataFrameWithLayerDeductionTestData( "layer_deduction_MultipleDeductionNonMonotonicMultipleofSameSeq.csv", sqlc )

    val expectedDeduction = populateDataFrameWithDeductionTestData( "deduction_MultipleDeductionNonMonotonicMultipleOfSameSeq.csv", sqlc )

    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    layerDeduction.registerTempTable( "layer_deduction" )

    sqlc.udf.register( "net_as_pct_of_gross", NetAsPctOfGross )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/Deduction.hql" )

    // Act
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert
    assertDataFrameEquals( expectedDeduction, result )

  }
}
