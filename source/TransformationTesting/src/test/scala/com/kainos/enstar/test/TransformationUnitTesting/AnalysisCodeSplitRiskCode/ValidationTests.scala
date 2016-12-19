package com.kainos.enstar.test.TransformationUnitTesting.AnalysisCodeSplitRiskCode

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, TransformationUnitTestingUtils }
import org.scalatest.FunSuite

class ValidationTests extends FunSuite with DataFrameSuiteBase {

  private val testDataInputPath = "/analysiscodesplit/validation/input/"

  test( "Validation: When input contains no null values for risk_reference validation should pass" ) {

    // Arrange //
    // Use sqlContext from spark-testing-base
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )
    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframe
    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_NoNulls.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Validation/AnalysisCodeSplitRiskCode/CheckForNullRiskReference.hql" )

    // Act //
    line.registerTempTable( "line" )
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assert( result.count() == 0 )
  }

  test( "Validation: When input contains rows with risk_reference null validation should fail" ) {

    // Arrange //
    // Use sqlContext from spark-testing-base
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )
    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframe
    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_NullRiskReference.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Validation/AnalysisCodeSplitRiskCode/CheckForNullRiskReference.hql" )

    // Act //
    line.registerTempTable( "line" )
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assert( result.count() == 1 )
  }

  test( "Validation: When every line has a corresponding line_risk_code then validation should pass" ){

    // Arrange //
    // Use sqlContext from spark-testing-base
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )
    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframe
    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_LineWithLineRiskCode.csv" )
    val lineRiskCode = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_risk_code_LineWithLineRiskCode.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Validation/AnalysisCodeSplitRiskCode/CheckForLineWithoutLineRiskCode.hql" )

    // Act //
    line.registerTempTable( "line" )
    lineRiskCode.registerTempTable( "line_risk_code" )
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assert( result.count() == 0 )
  }

  test( "When not every line has a corresponding line_risk_code then validation should fail" ){

    // Arrange //
    // Use sqlContext from spark-testing-base
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )
    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframe
    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_LineWithNoLineRiskCode.csv" )
    val lineRiskCode = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_risk_code_LineWithNoLineRiskCode.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Validation/AnalysisCodeSplitRiskCode/CheckForLineWithoutLineRiskCode.hql" )

    // Act //
    line.registerTempTable( "line" )
    lineRiskCode.registerTempTable( "line_risk_code" )
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assert( result.count() == 1 )

  }
}
