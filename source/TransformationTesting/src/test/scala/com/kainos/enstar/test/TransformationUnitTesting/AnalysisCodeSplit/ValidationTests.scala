package com.kainos.enstar.test.TransformationUnitTesting.AnalysisCodeSplit

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, TransformationUnitTestingUtils }
import org.scalatest.FunSuite

class ValidationTests extends FunSuite with DataFrameSuiteBase {

  private val utils = new TransformationUnitTestingUtils
  private val testDataInputPath = "/analysiscodesplit/validation/input/"

  ignore( "Validation: When input contains no null values for risk_reference validation should pass" ) {

    // Arrange //
    // Use sqlContext from spark-testing-base
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_NoNulls.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Validation/ndex/AnalysisCodeSplit/CheckForNullRiskReference.hql" )

    // Act //
    line.registerTempTable( "line" )
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assert( result.count() == 0 )
  }

  ignore( "Validation: When input contains rows with risk_reference null validation should fail" ) {

    // Arrange //
    // Use sqlContext from spark-testing-base
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_NullRiskReference.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Validation/ndex/AnalysisCodeSplit/CheckForNullRiskReference.hql" )

    // Act //
    line.registerTempTable( "line" )
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assert( result.count() == 1 )
  }

}