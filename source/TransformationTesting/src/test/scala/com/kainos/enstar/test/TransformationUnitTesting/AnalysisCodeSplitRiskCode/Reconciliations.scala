package com.kainos.enstar.test.TransformationUnitTesting.AnalysisCodeSplitRiskCode

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ AnalysisCodeSplitRiskCodeUtils, SQLRunner, TransformationUnitTestingUtils }
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

/**
 * Created by terences on 24/11/2016.
 */
class Reconciliations extends FunSuite with DataFrameSuiteBase {

  test( "Reconciliation on test case 1 input test data" ) {

    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Arrange //
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframe
    val line : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/analysiscodesplit_riskcode/input/line_test1.csv" ).toString,
      getClass.getResource( "/analysiscodesplit_riskcode/schemas/line.avro" ).toString,
      AnalysisCodeSplitRiskCodeUtils.lineMapping,
      sqlc
    )

    val line_risk_code : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/analysiscodesplit_riskcode/input/line_risk_code_test1.csv" ).toString,
      getClass.getResource( "/analysiscodesplit_riskcode/schemas/line_risk_code.avro" ).toString,
      AnalysisCodeSplitRiskCodeUtils.lineriskcodeMapping,
      sqlc
    )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/AnalysisCodeSplitRiskCode.hql" )
    val reconStatementInput = utils.loadHQLStatementFromResource( "Reconciliation/AnalysisCodeSplitRiskCode/InputRecordCount.hql" )
    val reconStatementOutput = utils.loadHQLStatementFromResource( "Reconciliation/AnalysisCodeSplitRiskCode/OutputRecordCount.hql" )

    // Act //
    line.registerTempTable( "line" )
    line_risk_code.registerTempTable( "line_risk_code" )
    val output = SQLRunner.runStatement( statement, sqlc )
    output.registerTempTable( "analysiscodesplit" )

    val reconInput = SQLRunner.runStatement( reconStatementInput, sqlc )
    val reconOutput = SQLRunner.runStatement( reconStatementOutput, sqlc )

    // Assert //
    assertDataFrameEquals( reconInput, reconOutput )
  }

  test( "Reconciliation on test case 2 input test data" ) {

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

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/AnalysisCodeSplitRiskCode.hql" )
    val reconStatementInput = utils.loadHQLStatementFromResource( "Reconciliation/AnalysisCodeSplitRiskCode/InputRecordCount.hql" )
    val reconStatementOutput = utils.loadHQLStatementFromResource( "Reconciliation/AnalysisCodeSplitRiskCode/OutputRecordCount.hql" )

    // Act //
    line.registerTempTable( "line" )
    line_risk_code.registerTempTable( "line_risk_code" )
    val output = SQLRunner.runStatement( statement, sqlc )
    output.registerTempTable( "analysiscodesplit" )

    val reconInput = SQLRunner.runStatement( reconStatementInput, sqlc )
    val reconOutput = SQLRunner.runStatement( reconStatementOutput, sqlc )

    // Assert //
    assertDataFrameEquals( reconInput, reconOutput )
  }
}
