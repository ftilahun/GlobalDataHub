package com.kainos.enstar.test.TransformationUnitTesting.AnalysisCodeSplitRiskCode

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ AnalysisCodeSplitRiskCodeUtils, SQLRunner, TransformationUnitTestingUtils }
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.scalatest.FunSuite

class ReconciliationTests extends FunSuite with DataFrameSuiteBase {

  private val utils = new TransformationUnitTestingUtils
  private val testDataInputPath = "/analysiscodesplit_riskcode/input/"
  private val testDataOutputPath = "/analysiscodesplit_riskcode/output/"
  private val schemasPath = "/analysiscodesplit_riskcode/schemas/"

  def populateDataFrameWithLineTestData( dataFileName : String, sqlc : SQLContext ) : DataFrame = {

    utils.populateDataFrameFromFile(
      getClass.getResource( testDataInputPath + dataFileName ).toString,
      getClass.getResource( schemasPath + "line.avro" ).toString,
      _.split( "," ),
      AnalysisCodeSplitRiskCodeUtils.lineMapping,
      sqlc
    )
  }

  def populateDataFrameWithLineRiskCodeTestData( dataFileName : String, sqlc : SQLContext ) : DataFrame = {

    utils.populateDataFrameFromFile(
      getClass.getResource( testDataInputPath + dataFileName ).toString,
      getClass.getResource( schemasPath + "line_risk_code.avro" ).toString,
      _.split( "," ),
      AnalysisCodeSplitRiskCodeUtils.lineRiskCodeMapping,
      sqlc
    )
  }

  def populateDataFrameWithLookupRiskCodeTestData( dataFileName : String, sqlc : SQLContext ) : DataFrame = {

    utils.populateDataFrameFromFile(
      getClass.getResource( testDataInputPath + dataFileName ).toString,
      getClass.getResource( schemasPath + "lookup_risk_code.avro" ).toString,
      _.split( "," ),
      AnalysisCodeSplitRiskCodeUtils.lookupRiskCodeMapping,
      sqlc
    )
  }

  def populateDataFrameWithAnalysisCodeSplitTestData( dataFileName : String, sqlc : SQLContext ) : DataFrame = {

    utils.populateDataFrameFromFile(
      getClass.getResource( testDataOutputPath + dataFileName ).toString,
      getClass.getResource( schemasPath + "analysiscodesplit.avro" ).toString,
      _.split( "," ),
      AnalysisCodeSplitRiskCodeUtils.analysisCodeSplitMapping,
      sqlc
    )
  }

  test( "Reconciliation on test case 1 input test data" ) {

    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Arrange //
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframe
    val line = this.populateDataFrameWithLineTestData( "line_PrimaryTestData.csv", sqlc )
    val line_risk_code = this.populateDataFrameWithLineRiskCodeTestData( "line_risk_code_PrimaryTestData.csv", sqlc )
    val lookup_risk_code = this.populateDataFrameWithLookupRiskCodeTestData( "lookup_risk_code_PrimaryTestData.csv", sqlc )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/AnalysisCodeSplitRiskCode.hql" )
    val reconStatementInput = utils.loadHQLStatementFromResource( "Reconciliation/AnalysisCodeSplitRiskCode/InputRecordCount.hql" )
    val reconStatementOutput = utils.loadHQLStatementFromResource( "Reconciliation/AnalysisCodeSplitRiskCode/OutputRecordCount.hql" )

    // Act //
    line.registerTempTable( "line" )
    line_risk_code.registerTempTable( "line_risk_code" )
    lookup_risk_code.registerTempTable( "lookup_risk_code" )
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
    val line = this.populateDataFrameWithLineTestData( "line_ManyLineRiskToOneLine.csv", sqlc )
    val line_risk_code = this.populateDataFrameWithLineRiskCodeTestData( "line_risk_code_ManyLineRiskToOneLine.csv", sqlc )
    val lookup_risk_code = this.populateDataFrameWithLookupRiskCodeTestData( "lookup_risk_code_PrimaryTestData.csv", sqlc )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/AnalysisCodeSplitRiskCode.hql" )
    val reconStatementInput = utils.loadHQLStatementFromResource( "Reconciliation/AnalysisCodeSplitRiskCode/InputRecordCount.hql" )
    val reconStatementOutput = utils.loadHQLStatementFromResource( "Reconciliation/AnalysisCodeSplitRiskCode/OutputRecordCount.hql" )

    // Act //
    line.registerTempTable( "line" )
    line_risk_code.registerTempTable( "line_risk_code" )
    lookup_risk_code.registerTempTable( "lookup_risk_code" )
    val output = SQLRunner.runStatement( statement, sqlc )
    output.registerTempTable( "analysiscodesplit" )

    val reconInput = SQLRunner.runStatement( reconStatementInput, sqlc )
    val reconOutput = SQLRunner.runStatement( reconStatementOutput, sqlc )

    // Assert //
    assertDataFrameEquals( reconInput, reconOutput )
  }
}
