package com.kainos.enstar.test.TransformationUnitTesting.Deduction

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ NetAsPctOfGross, SQLRunner, TransformationUnitTestingUtils }
import org.scalatest.FunSuite

/**
 * Created by terences on 08/12/2016.
 */
class TransformationTests extends FunSuite with DataFrameSuiteBase {

  private val utils = new TransformationUnitTestingUtils

  test( "Deduction transformation mapping test with primary data sets" ){

    // Arrange
    implicit val sqlc = sqlContext
    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframes
    val line = utils.populateDataFrameFromCsvWithHeader( "/deduction/input/line_PrimaryTestData.csv" )
    val layer = utils.populateDataFrameFromCsvWithHeader( "/deduction/input/layer_PrimaryTestData.csv" )
    val layerDeduction = utils.populateDataFrameFromCsvWithHeader( "/deduction/input/layer_deduction_PrimaryTestData.csv" )

    val expectedDeduction = utils.populateDataFrameFromCsvWithHeader( "/deduction/output/deduction_PrimaryTestData.csv" )

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
    implicit val sqlc = sqlContext
    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframes
    val line = utils.populateDataFrameFromCsvWithHeader( "/deduction/input/line_SingleDeductionCalculatedDeductionAmount.csv" )
    val layer = utils.populateDataFrameFromCsvWithHeader( "/deduction/input/layer_SingleDeductionCalculatedDeductionAmount.csv" )
    val layerDeduction = utils.populateDataFrameFromCsvWithHeader( "/deduction/input/layer_deduction_SingleDeductionCalculatedDeductionAmount.csv" )

    val expectedDeduction = utils.populateDataFrameFromCsvWithHeader( "/deduction/output/deduction_SingleDeductionCalculatedDeductionAmount.csv" )

    sqlc.udf.register( "net_as_pct_of_gross", NetAsPctOfGross )

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
    implicit val sqlc = sqlContext
    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframes
    val line = utils.populateDataFrameFromCsvWithHeader( "/deduction/input/line_PrimaryTestData.csv" )
    val layer = utils.populateDataFrameFromCsvWithHeader( "/deduction/input/layer_PrimaryTestData.csv" )
    val layerDeduction = utils.populateDataFrameFromCsvWithHeader( "/deduction/input/layer_deduction_MultipleDeductionMonotonicSeq.csv" )

    val expectedDeduction = utils.populateDataFrameFromCsvWithHeader( "/deduction/output/deduction_MultipleDeductionMonotonicSeq.csv" )

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
    implicit val sqlc = sqlContext
    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframes
    val line = utils.populateDataFrameFromCsvWithHeader( "/deduction/input/line_MultipleLines.csv" )
    val layer = utils.populateDataFrameFromCsvWithHeader( "/deduction/input/layer_MultipleLines.csv" )
    val layerDeduction = utils.populateDataFrameFromCsvWithHeader( "/deduction/input/layer_deduction_MultipleDeductionMonotonicSeqMultipleLines.csv" )

    val expectedDeduction = utils.populateDataFrameFromCsvWithHeader( "/deduction/output/deduction_MultipleDeductionMonotonicSeqMultipleLines.csv" )

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
    implicit val sqlc = sqlContext
    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframes
    val line = utils.populateDataFrameFromCsvWithHeader( "/deduction/input/line_PrimaryTestData.csv" )
    val layer = utils.populateDataFrameFromCsvWithHeader( "/deduction/input/layer_PrimaryTestData.csv" )
    val layerDeduction = utils.populateDataFrameFromCsvWithHeader( "/deduction/input/layer_deduction_MultipleDeductionAllSameSeqNo.csv" )

    val expectedDeduction = utils.populateDataFrameFromCsvWithHeader( "/deduction/output/deduction_MultipleDeductionAllSameSeqNo.csv" )

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
    implicit val sqlc = sqlContext
    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframes
    val line = utils.populateDataFrameFromCsvWithHeader( "/deduction/input/line_PrimaryTestData.csv" )
    val layer = utils.populateDataFrameFromCsvWithHeader( "/deduction/input/layer_PrimaryTestData.csv" )
    val layerDeduction = utils.populateDataFrameFromCsvWithHeader( "/deduction/input/layer_deduction_MultipleDeductionNonMonotonicSeq.csv" )

    val expectedDeduction = utils.populateDataFrameFromCsvWithHeader( "/deduction/output/deduction_MultipleDeductionNonMonotonicSeq.csv" )

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
    implicit val sqlc = sqlContext
    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframes
    val line = utils.populateDataFrameFromCsvWithHeader( "/deduction/input/line_PrimaryTestData.csv" )
    val layer = utils.populateDataFrameFromCsvWithHeader( "/deduction/input/layer_PrimaryTestData.csv" )
    val layerDeduction = utils.populateDataFrameFromCsvWithHeader( "/deduction/input/layer_deduction_MultipleDeductionNonMonotonicSeqOutOfOrder.csv" )

    val expectedDeduction = utils.populateDataFrameFromCsvWithHeader( "/deduction/output/deduction_MultipleDeductionNonMonotonicSeqOutOfOrder.csv" )

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
    implicit val sqlc = sqlContext
    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframes
    val line = utils.populateDataFrameFromCsvWithHeader( "/deduction/input/line_PrimaryTestData.csv" )
    val layer = utils.populateDataFrameFromCsvWithHeader( "/deduction/input/layer_PrimaryTestData.csv" )
    val layerDeduction = utils.populateDataFrameFromCsvWithHeader( "/deduction/input/layer_deduction_MultipleDeductionNonMonotonicMultipleofSameSeq.csv" )

    val expectedDeduction = utils.populateDataFrameFromCsvWithHeader( "/deduction/output/deduction_MultipleDeductionNonMonotonicMultipleOfSameSeq.csv" )

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
