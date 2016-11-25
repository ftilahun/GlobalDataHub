package com.kainos.enstar.test.TransformationUnitTesting.AnalysisCodeSplitRiskCode

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ AnalysisCodeSplitRiskCodeUtils, SQLRunner, TransformationUnitTestingUtils }
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

/**
 * Created by terences on 23/11/2016.
 */
class TransformationTests extends FunSuite with DataFrameSuiteBase {

  test( "AnalysisCodeSplitRiskCodeTransformation mapping test one line row to one line_risk_code row" ){

    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Arrange //
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframe
    val line : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/analysiscodesplit_riskcode/input/line_test1.csv" ).toString,
      getClass.getResource( "/analysiscodesplit_riskcode/schemas/line.avro" ).toString,
      _.split( "," ),
      AnalysisCodeSplitRiskCodeUtils.lineMapping,
      sqlc
    )

    val line_risk_code : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/analysiscodesplit_riskcode/input/line_risk_code_test1.csv" ).toString,
      getClass.getResource( "/analysiscodesplit_riskcode/schemas/line_risk_code.avro" ).toString,
      _.split( "," ),
      AnalysisCodeSplitRiskCodeUtils.lineriskcodeMapping,
      sqlc
    )

    // Load expected result into dataframe
    val expectedAnalysisCodeSplit : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/analysiscodesplit_riskcode/output/analysiscodesplit.csv" ).toString,
      getClass.getResource( "/analysiscodesplit_riskcode/schemas/analysiscodesplit.avro" ).toString,
      _.split( "," ),
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

  test( "AnalysisCodeSplitRiskCodeTransformation mapping test many line_risk_code rows to one line row" ){

    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Arrange //
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframe
    val line : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/analysiscodesplit_riskcode/input/line_test2.csv" ).toString,
      getClass.getResource( "/analysiscodesplit_riskcode/schemas/line.avro" ).toString,
      _.split( "," ),
      AnalysisCodeSplitRiskCodeUtils.lineMapping,
      sqlc
    )

    val line_risk_code : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/analysiscodesplit_riskcode/input/line_risk_code_test2.csv" ).toString,
      getClass.getResource( "/analysiscodesplit_riskcode/schemas/line_risk_code.avro" ).toString,
      _.split( "," ),
      AnalysisCodeSplitRiskCodeUtils.lineriskcodeMapping,
      sqlc
    )

    // Load expected result into dataframe
    val expectedAnalysisCodeSplit : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/analysiscodesplit_riskcode/output/analysiscodesplit_test2.csv" ).toString,
      getClass.getResource( "/analysiscodesplit_riskcode/schemas/analysiscodesplit.avro" ).toString,
      _.split( "," ),
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
