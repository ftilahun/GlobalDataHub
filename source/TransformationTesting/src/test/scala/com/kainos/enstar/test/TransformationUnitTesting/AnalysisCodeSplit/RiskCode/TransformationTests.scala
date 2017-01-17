package com.kainos.enstar.test.TransformationUnitTesting.AnalysisCodeSplit.RiskCode

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, TransformationUnitTestingUtils }
import org.scalatest.FunSuite

class TransformationTests extends FunSuite with DataFrameSuiteBase {

  private val utils = new TransformationUnitTestingUtils
  private val testDataInputDirPath = "/analysiscodesplit/riskcode/input/"
  private val testDataOutputDirPath = "/analysiscodesplit/riskcode/output/"
  private val analysisCodeSplitRiskCodeTransformationPath = "Transformation/ndex/AnalysisCodeSplitRiskCode.hql"

  test( "AnalysisCodeSplitRiskCodeTransformation mapping test one line row to one line_risk_code row" ){

    implicit val sqlc = sqlContext
    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Arrange //
    // Load test data into dataframe
    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "line_PrimaryTestData.csv" )
    val line_risk_code = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "line_risk_code_PrimaryTestData.csv" )
    val lookup_risk_code = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "lookup_risk_code_PrimaryTestData.csv" )

    // Load expected result into dataframe
    val expectedAnalysisCodeSplit = utils.populateDataFrameFromCsvWithHeader( testDataOutputDirPath + "analysiscodesplit_PrimaryTestData.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( analysisCodeSplitRiskCodeTransformationPath )

    // Act //
    line.registerTempTable( "line" )
    line_risk_code.registerTempTable( "line_risk_code" )
    lookup_risk_code.registerTempTable( "lookup_risk_code" )
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedAnalysisCodeSplit.orderBy( "policynumber", "coveragereference" ), result.orderBy( "policynumber", "coveragereference" ) )
  }

  test( "AnalysisCodeSplitRiskCodeTransformation mapping test many line_risk_code rows to one line row" ){

    implicit val sqlc = sqlContext

    // Arrange //
    // Load test data into dataframe
    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "line_ManyLineRiskToOneLine.csv" )
    val line_risk_code = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "line_risk_code_ManyLineRiskToOneLine.csv" )
    val lookup_risk_code = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "lookup_risk_code_ManyLineRiskToOneLine.csv" )

    // Load expected result into dataframe
    val expectedAnalysisCodeSplit = utils.populateDataFrameFromCsvWithHeader( testDataOutputDirPath + "analysiscodesplit_ManyLineRiskToOneLine.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( analysisCodeSplitRiskCodeTransformationPath )

    // Act //
    line.registerTempTable( "line" )
    line_risk_code.registerTempTable( "line_risk_code" )
    lookup_risk_code.registerTempTable( "lookup_risk_code" )
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedAnalysisCodeSplit.orderBy( "policynumber", "analysiscode" ), result.orderBy( "policynumber", "analysiscode" ) )
  }
}
