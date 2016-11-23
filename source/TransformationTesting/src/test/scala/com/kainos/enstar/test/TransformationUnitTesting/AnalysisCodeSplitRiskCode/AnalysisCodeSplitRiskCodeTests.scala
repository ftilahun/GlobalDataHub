package com.kainos.enstar.test.TransformationUnitTesting.AnalysisCodeSplitRiskCode

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ AnalysisCodeSplitRiskCodeUtils, SQLRunner, TransformationUnitTestingUtils }
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

/**
 * Created by terences on 23/11/2016.
 */
class AnalysisCodeSplitRiskCodeTests extends FunSuite with DataFrameSuiteBase {

  test( "AnalysisCodeSplitRiskCodeTransformation_test1" ){

    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Arrange //
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframe
    val line : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/analysiscodesplit_riskcode/input/line.csv" ).toString,
      getClass.getResource( "/analysiscodesplit_riskcode/schemas/line.avro" ).toString,
      AnalysisCodeSplitRiskCodeUtils.lineMapping,
      sqlc
    )

    val line_risk_code : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/analysiscodesplit_riskcode/input/line_risk_code.csv" ).toString,
      getClass.getResource( "/analysiscodesplit_riskcode/schemas/line_risk_code.avro" ).toString,
      AnalysisCodeSplitRiskCodeUtils.lineriskcodeMapping,
      sqlc
    )

    // Load expected result into dataframe
    val expectedAnalysisCodeSplit : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/analysiscodesplit_riskcode/output/analysiscodesplit.csv" ).toString,
      getClass.getResource( "/analysiscodesplit_riskcode/schemas/analysiscodesplit.avro" ).toString,
      AnalysisCodeSplitRiskCodeUtils.analysiscodesplitMapping,
      sqlc
    )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/AnalysisCodeSplitRiskCode.hql" )

    // Act //
    line.registerTempTable( "line" )
    line_risk_code.registerTempTable( "line_risk_code" )
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedAnalysisCodeSplit, result )
  }

  test( "AnalysisCodeSplitRiskCodeTransformation_test2" ){

    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Arrange //
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframe
    val line : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/analysiscodesplit_riskcode/input/line_test2.csv" ).toString,
      getClass.getResource( "/analysiscodesplit_riskcode/schemas/line.avro" ).toString,
      AnalysisCodeSplitRiskCodeUtils.lineMapping,
      sqlc
    )

    val line_risk_code : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/analysiscodesplit_riskcode/input/line_risk_code_test2.csv" ).toString,
      getClass.getResource( "/analysiscodesplit_riskcode/schemas/line_risk_code.avro" ).toString,
      AnalysisCodeSplitRiskCodeUtils.lineriskcodeMapping,
      sqlc
    )

    // Load expected result into dataframe
    val expectedAnalysisCodeSplit : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/analysiscodesplit_riskcode/output/analysiscodesplit_test2.csv" ).toString,
      getClass.getResource( "/analysiscodesplit_riskcode/schemas/analysiscodesplit.avro" ).toString,
      AnalysisCodeSplitRiskCodeUtils.analysiscodesplitMapping,
      sqlc
    )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/AnalysisCodeSplitRiskCode.hql" )

    // Act //
    line.registerTempTable( "line" )
    line_risk_code.registerTempTable( "line_risk_code" )
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedAnalysisCodeSplit, result )
  }
}
