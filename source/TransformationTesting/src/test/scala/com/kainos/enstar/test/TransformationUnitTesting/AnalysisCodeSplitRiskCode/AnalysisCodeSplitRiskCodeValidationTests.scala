package com.kainos.enstar.test.TransformationUnitTesting.AnalysisCodeSplitRiskCode

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ AnalysisCodeSplitRiskCodeUtils, SQLRunner, TransformationUnitTestingUtils }
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

/**
 * Created by terences on 23/11/2016.
 */
class AnalysisCodeSplitRiskCodeValidationTests extends FunSuite with DataFrameSuiteBase {

  test( "Validation: When input contains no null values for risk_reference validation should pass" ) {

    // Arrange //
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )
    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframe
    val line : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/analysiscodesplit_riskcode_validation/input/line_test1.csv" ).toString,
      getClass.getResource( "/analysiscodesplit_riskcode_validation/schema/line.avro" ).toString,
      AnalysisCodeSplitRiskCodeUtils.lineMapping,
      sqlc
    )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Validation/AnalysisCodeSplitRiskCodeValidation.hql" )

    // Act //
    line.registerTempTable( "line" )
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assert( result.count() == 0 )
  }

  test( "Validation: When input contains rows with risk_reference null validation should fail" ) {

    // Arrange //
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )
    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframe
    val line : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/analysiscodesplit_riskcode_validation/input/line_test2.csv" ).toString,
      getClass.getResource( "/analysiscodesplit_riskcode_validation/schema/line.avro" ).toString,
      AnalysisCodeSplitRiskCodeUtils.lineMapping,
      sqlc
    )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Validation/AnalysisCodeSplitRiskCodeValidation.hql" )

    // Act //
    line.registerTempTable( "line" )
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assert( result.count() == 1 )
  }
}
