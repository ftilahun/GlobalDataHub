package com.kainos.enstar.test.TransformationUnitTesting.Policy

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, TransformationUnitTestingUtils }
import org.scalatest.FunSuite

class ValidationTests extends FunSuite with DataFrameSuiteBase {

  private val utils = new TransformationUnitTestingUtils
  private val testDataInputPath = "/policy/validation/input/"

  test( "Validation: When input contains no null values for risk_reference validation script should return count 0" ) {

    // Arrange //
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_NoNullRiskReference.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Validation/Policy/CheckForNullRiskReference.hql" )

    // Act //
    line.registerTempTable( "line" )
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assert( result.count() == 0 )
  }

  test( "Validation: When input contains rows with risk_reference null validation script should return count 1" ) {

    // Arrange //
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_NullRiskReference.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Validation/Policy/CheckForNullRiskReference.hql" )

    // Act //
    line.registerTempTable( "line" )
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assert( result.count() == 1 )
  }

}
