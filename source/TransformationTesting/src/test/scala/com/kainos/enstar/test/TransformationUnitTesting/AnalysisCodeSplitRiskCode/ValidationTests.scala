package com.kainos.enstar.test.TransformationUnitTesting.AnalysisCodeSplitRiskCode

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ AnalysisCodeSplitRiskCodeUtils, SQLRunner, TransformationUnitTestingUtils }
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

class ValidationTests extends FunSuite with DataFrameSuiteBase {

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
      _.split( "," ),
      AnalysisCodeSplitRiskCodeUtils.lineMapping,
      sqlc
    )

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
    val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )
    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframe
    val line : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/analysiscodesplit_riskcode_validation/input/line_test2.csv" ).toString,
      getClass.getResource( "/analysiscodesplit_riskcode_validation/schema/line.avro" ).toString,
      _.split( "," ),
      AnalysisCodeSplitRiskCodeUtils.lineMapping,
      sqlc
    )

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
    val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )
    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframe
    val line : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/analysiscodesplit_riskcode_validation/input/line_test3.csv" ).toString,
      getClass.getResource( "/analysiscodesplit_riskcode_validation/schema/line.avro" ).toString,
      _.split( "," ),
      AnalysisCodeSplitRiskCodeUtils.lineMapping,
      sqlc
    )

    val lineRiskCode : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/analysiscodesplit_riskcode_validation/input/line_risk_code_test3.csv" ).toString,
      getClass.getResource( "/analysiscodesplit_riskcode_validation/schema/line_risk_code.avro" ).toString,
      _.split( "," ),
      AnalysisCodeSplitRiskCodeUtils.lineRiskCodeMapping,
      sqlc
    )

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
    val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )
    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframe
    val line : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/analysiscodesplit_riskcode_validation/input/line_test4.csv" ).toString,
      getClass.getResource( "/analysiscodesplit_riskcode_validation/schema/line.avro" ).toString,
      _.split( "," ),
      AnalysisCodeSplitRiskCodeUtils.lineMapping,
      sqlc
    )

    val lineRiskCode : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/analysiscodesplit_riskcode_validation/input/line_risk_code_test4.csv" ).toString,
      getClass.getResource( "/analysiscodesplit_riskcode_validation/schema/line_risk_code.avro" ).toString,
      _.split( "," ),
      AnalysisCodeSplitRiskCodeUtils.lineRiskCodeMapping,
      sqlc
    )

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
