package com.kainos.enstar.test.TransformationUnitTesting.Deduction

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ NetAsPctOfGross, SQLRunner, TransformationUnitTestingUtils }
import org.scalatest.FunSuite

/**
 * Created by terences on 19/12/2016.
 */
class ReconciliationTests extends FunSuite with DataFrameSuiteBase {

  private val utils = new TransformationUnitTestingUtils

  test( "Reconciliation over test data" ){

    // Arrange
    implicit val sqlc = sqlContext
    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframes
    val line = utils.populateDataFrameFromCsvWithHeader( "/deduction/input/line_PrimaryTestData.csv" )
    val layer = utils.populateDataFrameFromCsvWithHeader( "/deduction/input/layer_PrimaryTestData.csv" )
    val layerDeduction = utils.populateDataFrameFromCsvWithHeader( "/deduction/input/layer_deduction_MultipleDeductionNonMonotonicSeqOutOfOrder.csv" )

    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    layerDeduction.registerTempTable( "layer_deduction" )

    sqlc.udf.register( "net_as_pct_of_gross", NetAsPctOfGross )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/ndex/Deduction.hql" )
    val reconStatementInput = utils.loadHQLStatementFromResource( "Reconciliation/Deduction/InputRecordCount.hql" )
    val reconStatementOutput = utils.loadHQLStatementFromResource( "Reconciliation/Deduction/OutputRecordCount.hql" )

    // Act
    val result = SQLRunner.runStatement( statement, sqlc )
    result.registerTempTable( "deduction" )

    val reconInput = SQLRunner.runStatement( reconStatementInput, sqlc )
    val reconOutput = SQLRunner.runStatement( reconStatementOutput, sqlc )

    // Assert
    assertDataFrameEquals( reconInput, reconOutput )

  }

  test( "Reconciliation over test data with multiple lines" ){

    // Arrange
    implicit val sqlc = sqlContext
    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframes
    val line = utils.populateDataFrameFromCsvWithHeader( "/deduction/input/line_MultipleLines.csv" )
    val layer = utils.populateDataFrameFromCsvWithHeader( "/deduction/input/layer_MultipleLines.csv" )
    val layerDeduction = utils.populateDataFrameFromCsvWithHeader( "/deduction/input/layer_deduction_MultipleDeductionMonotonicSeqMultipleLines.csv" )

    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    layerDeduction.registerTempTable( "layer_deduction" )

    sqlc.udf.register( "net_as_pct_of_gross", NetAsPctOfGross )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/ndex/Deduction.hql" )
    val reconStatementInput = utils.loadHQLStatementFromResource( "Reconciliation/Deduction/InputRecordCount.hql" )
    val reconStatementOutput = utils.loadHQLStatementFromResource( "Reconciliation/Deduction/OutputRecordCount.hql" )

    // Act
    val result = SQLRunner.runStatement( statement, sqlc )
    result.registerTempTable( "deduction" )

    val reconInput = SQLRunner.runStatement( reconStatementInput, sqlc )
    val reconOutput = SQLRunner.runStatement( reconStatementOutput, sqlc )

    // Assert
    assertDataFrameEquals( reconInput, reconOutput )

  }
}
