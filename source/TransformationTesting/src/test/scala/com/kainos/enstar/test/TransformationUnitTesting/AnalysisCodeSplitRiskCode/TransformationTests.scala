package com.kainos.enstar.test.TransformationUnitTesting.AnalysisCodeSplitRiskCode

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ AnalysisCodeSplitRiskCodeUtils, SQLRunner, TransformationUnitTestingUtils }
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.scalatest.FunSuite

class TransformationTests extends FunSuite with DataFrameSuiteBase {

  private val utils = new TransformationUnitTestingUtils
  private val testDataInputPath = "/analysiscodesplit_riskcode/input/"
  private val testDataOutputPath = "/analysiscodesplit_riskcode/output/"
  private val schemasPath = "/analysiscodesplit_riskcode/schemas/"
  private val analysisCodeSplitRiskCodeTransformation = "Transformation/AnalysisCodeSplitRiskCode.hql"

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

  test( "AnalysisCodeSplitRiskCodeTransformation mapping test one line row to one line_risk_code row" ){

    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Arrange //
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframes
    val line = this.populateDataFrameWithLineTestData( "line_PrimaryTestData.csv", sqlc )
    val line_risk_code = this.populateDataFrameWithLineRiskCodeTestData( "line_risk_code_PrimaryTestData.csv", sqlc )
    val lookup_risk_code = this.populateDataFrameWithLookupRiskCodeTestData( "lookup_risk_code_PrimaryTestData.csv", sqlc )

    // Load expected result into dataframe
    val expectedAnalysisCodeSplit = this.populateDataFrameWithAnalysisCodeSplitTestData( "analysiscodesplit_PrimaryTestData.csv", sqlc )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( analysisCodeSplitRiskCodeTransformation )

    // Act //
    line.registerTempTable( "line" )
    line_risk_code.registerTempTable( "line_risk_code" )
    lookup_risk_code.registerTempTable( "lookup_risk_code" )
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedAnalysisCodeSplit.orderBy( "policynumber", "coveragereference" ), result.orderBy( "policynumber", "coveragereference" ) )
  }

  test( "AnalysisCodeSplitRiskCodeTransformation mapping test many line_risk_code rows to one line row" ){

    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Arrange //
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframe
    val line = this.populateDataFrameWithLineTestData( "line_ManyLineRiskToOneLine.csv", sqlc )
    val line_risk_code = this.populateDataFrameWithLineRiskCodeTestData( "line_risk_code_ManyLineRiskToOneLine.csv", sqlc )
    val lookup_risk_code = this.populateDataFrameWithLookupRiskCodeTestData( "lookup_risk_code_ManyLineRiskToOneLine.csv", sqlc )

    // Load expected result into dataframe
    val expectedAnalysisCodeSplit = this.populateDataFrameWithAnalysisCodeSplitTestData( "analysiscodesplit_ManyLineRiskToOneLine.csv", sqlc )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( analysisCodeSplitRiskCodeTransformation )

    // Act //
    line.registerTempTable( "line" )
    line_risk_code.registerTempTable( "line_risk_code" )
    lookup_risk_code.registerTempTable( "lookup_risk_code" )
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedAnalysisCodeSplit.orderBy( "policynumber", "analysiscode" ), result.orderBy( "policynumber", "analysiscode" ) )
  }
}
