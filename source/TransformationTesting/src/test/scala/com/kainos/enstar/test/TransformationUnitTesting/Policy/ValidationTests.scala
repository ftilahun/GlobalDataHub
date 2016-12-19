package com.kainos.enstar.test.TransformationUnitTesting.Policy

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ PolicyUtils, SQLRunner, TransformationUnitTestingUtils }
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

class ValidationTests extends FunSuite with DataFrameSuiteBase {

  private val utils = new TransformationUnitTestingUtils
  private val testDataInputPath = "/policy_validation/input/"
  private val schemasPath = "/policy_validation/schemas/"

  test( "Validation: When input contains no null values for risk_reference validation script should return count 0" ) {

    // Arrange //
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( testDataInputPath + "line_NoNullRiskReference.csv" ).toString,
      getClass.getResource( schemasPath + "line.avro" ).toString,
      _.split( "," ),
      PolicyUtils.lineMapping,
      sqlc
    )

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
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( testDataInputPath + "line_NullRiskReference.csv" ).toString,
      getClass.getResource( schemasPath + "line.avro" ).toString,
      _.split( "," ),
      PolicyUtils.lineMapping,
      sqlc
    )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Validation/Policy/CheckForNullRiskReference.hql" )

    // Act //
    line.registerTempTable( "line" )
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assert( result.count() == 1 )
  }

}
