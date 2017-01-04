package com.kainos.enstar.test.TransformationUnitTesting.Policy

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, TransformationUnitTestingUtils }
import org.scalatest.FunSuite

class TransformationTests extends FunSuite with DataFrameSuiteBase {

  private val utils = new TransformationUnitTestingUtils
  private val testDataInputPath = "/policy/input/"
  private val testDataOutputPath = "/policy/output/"
  private val policyTransformation = "Transformation/Policy.hql"

  test( "Policy Transformation mapping happy path data" ) {

    // Arrange //
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_PrimaryTestData.csv" )
    val layer = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "layer_PrimaryTestData.csv" )
    val submission = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "submission_PrimaryTestData.csv" )
    val risk = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "risk_PrimaryTestData.csv" )
    val organisation = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "organisation_PrimaryTestData.csv" )
    val lookup_block = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "lookup_block_PrimaryTestData.csv" )
    val lookup_business_type = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "lookup_business_type_PrimaryTestData.csv" )
    val lookup_profit_centre = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "lookup_profit_centre_PrimaryTestData.csv" )
    val underwriting_block = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "underwriting_block_PrimaryTestData.csv" )

    // Load expected result into dataframe
    val expectedPolicy = utils.populateDataFrameFromCsvWithHeader( testDataOutputPath + "policy_PrimaryTestData.csv")

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( policyTransformation )

    // Act //
    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    submission.registerTempTable( "submission" )
    risk.registerTempTable( "risk" )
    organisation.registerTempTable( "organisation" )
    lookup_business_type.registerTempTable( "lookup_business_type" )
    lookup_block.registerTempTable( "lookup_block" )
    lookup_profit_centre.registerTempTable( "lookup_profit_centre" )
    underwriting_block.registerTempTable( "underwriting_block" )

    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicy, result )

  }

  test( "PolicyTransformation test with null inception date in layer" ) {

    // Arrange //
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_PrimaryTestData.csv" )
    val layer = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "layer_NullInceptionDate.csv" )
    val submission = utils.populateDataFrameFromCsvWithHeader ( testDataInputPath + "submission_PrimaryTestData.csv")
    val risk = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "risk_PrimaryTestData.csv")
    val organisation = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "organisation_PrimaryTestData.csv" )
    val lookup_block = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "lookup_block_PrimaryTestData.csv" )
    val lookup_business_type = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "lookup_business_type_PrimaryTestData.csv" )
    val lookup_profit_centre = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "lookup_profit_centre_PrimaryTestData.csv" )
    val underwriting_block = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "underwriting_block_PrimaryTestData.csv" )

    // Load expected result into dataframe
    val expectedPolicy = utils.populateDataFrameFromCsvWithHeader( testDataOutputPath + "policy_NullInceptionDateInLayer.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( policyTransformation )

    // Act //
    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    submission.registerTempTable( "submission" )
    risk.registerTempTable( "risk" )
    organisation.registerTempTable( "organisation" )
    lookup_business_type.registerTempTable( "lookup_business_type" )
    lookup_block.registerTempTable( "lookup_block" )
    lookup_profit_centre.registerTempTable( "lookup_profit_centre" )
    underwriting_block.registerTempTable( "underwriting_block" )

    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicy, result )
  }

  test( "PolicyTransformation test with null expiry date in layer" ) {

    // Arrange //
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_PrimaryTestData.csv" )
    val layer = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "layer_NullExpiryDate.csv" )
    val submission = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "submission_PrimaryTestData.csv" )
    val risk = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "risk_PrimaryTestData.csv" )
    val organisation = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "organisation_PrimaryTestData.csv" )
    val lookup_block = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "lookup_block_PrimaryTestData.csv" )
    val lookup_business_type = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "lookup_business_type_PrimaryTestData.csv" )
    val lookup_profit_centre = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "lookup_profit_centre_PrimaryTestData.csv" )
    val underwriting_block = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "underwriting_block_PrimaryTestData.csv" )

    // Load expected result into dataframe
    val expectedPolicy = utils.populateDataFrameFromCsvWithHeader( testDataOutputPath + "policy_NullExpiryDateInLayer.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( policyTransformation )

    // Act //
    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    submission.registerTempTable( "submission" )
    risk.registerTempTable( "risk" )
    organisation.registerTempTable( "organisation" )
    lookup_business_type.registerTempTable( "lookup_business_type" )
    lookup_block.registerTempTable( "lookup_block" )
    lookup_profit_centre.registerTempTable( "lookup_profit_centre" )
    underwriting_block.registerTempTable( "underwriting_block" )

    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicy, result )
  }

  test( "PolicyTransformation test with null values in layer inception date, expiry date, filcode, unique market ref" ) {

    // Arrange //
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_PrimaryTestData.csv" )
    val layer = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "layer_VariousNullValues.csv" )
    val submission = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "submission_PrimaryTestData.csv" )
    val risk = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "risk_PrimaryTestData.csv" )
    val organisation = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "organisation_PrimaryTestData.csv" )
    val lookup_block = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "lookup_block_PrimaryTestData.csv" )
    val lookup_business_type = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "lookup_business_type_PrimaryTestData.csv" )
    val lookup_profit_centre = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "lookup_profit_centre_PrimaryTestData.csv" )
    val underwriting_block = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "underwriting_block_PrimaryTestData.csv" )

    // Load expected result into dataframe
    val expectedPolicy = utils.populateDataFrameFromCsvWithHeader( testDataOutputPath + "policy_VariousNullsInLayer.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( policyTransformation )

    // Act //
    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    submission.registerTempTable( "submission" )
    risk.registerTempTable( "risk" )
    organisation.registerTempTable( "organisation" )
    lookup_business_type.registerTempTable( "lookup_business_type" )
    lookup_block.registerTempTable( "lookup_block" )
    lookup_profit_centre.registerTempTable( "lookup_profit_centre" )
    underwriting_block.registerTempTable( "underwriting_block" )

    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicy, result )
  }

  test( "PolicyTransformation test with no link between line and lookup profit centre" ) {

    // Arrange //
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_NullProfitCentreCode.csv" )
    val layer = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "layer_PrimaryTestData.csv" )
    val submission = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "submission_PrimaryTestData.csv" )
    val risk = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "risk_PrimaryTestData.csv" )
    val organisation = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "organisation_PrimaryTestData.csv" )
    val lookup_block = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "lookup_block_PrimaryTestData.csv" )
    val lookup_business_type = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "lookup_business_type_PrimaryTestData.csv" )
    val lookup_profit_centre = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "lookup_profit_centre_PrimaryTestData.csv" )
    val underwriting_block = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "underwriting_block_PrimaryTestData.csv" )

    // Load expected result into dataframe
    val expectedPolicy = utils.populateDataFrameFromCsvWithHeader( testDataOutputPath + "policy_NullProfitCentreCodeInLine.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( policyTransformation )

    // Act //
    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    submission.registerTempTable( "submission" )
    risk.registerTempTable( "risk" )
    organisation.registerTempTable( "organisation" )
    lookup_business_type.registerTempTable( "lookup_business_type" )
    lookup_block.registerTempTable( "lookup_block" )
    lookup_profit_centre.registerTempTable( "lookup_profit_centre" )
    underwriting_block.registerTempTable( "underwriting_block" )

    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicy, result )
  }

  test( "PolicyTransformation test with null block in line" ) {

    // Arrange //
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_NullBlock.csv" )
    val layer = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "layer_PrimaryTestData.csv" )
    val submission = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "submission_PrimaryTestData.csv" )
    val risk = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "risk_PrimaryTestData.csv" )
    val organisation = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "organisation_PrimaryTestData.csv" )
    val lookup_block = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "lookup_block_PrimaryTestData.csv" )
    val lookup_business_type = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "lookup_business_type_PrimaryTestData.csv" )
    val lookup_profit_centre = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "lookup_profit_centre_PrimaryTestData.csv" )
    val underwriting_block = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "underwriting_block_PrimaryTestData.csv" )

    // Load expected result into dataframe
    val expectedPolicy = utils.populateDataFrameFromCsvWithHeader( testDataOutputPath + "policy_NullBlockInLine.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( policyTransformation )

    // Act //
    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    submission.registerTempTable( "submission" )
    risk.registerTempTable( "risk" )
    organisation.registerTempTable( "organisation" )
    lookup_business_type.registerTempTable( "lookup_business_type" )
    lookup_block.registerTempTable( "lookup_block" )
    lookup_profit_centre.registerTempTable( "lookup_profit_centre" )
    underwriting_block.registerTempTable( "underwriting_block" )

    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicy, result )
  }

  test( "PolicyTransformation test with line status not equal to C" ) {

    // Arrange //
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_LineStatusNotC.csv" )
    val layer = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "layer_PrimaryTestData.csv" )
    val submission = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "submission_PrimaryTestData.csv" )
    val risk = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "risk_PrimaryTestData.csv" )
    val organisation = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "organisation_PrimaryTestData.csv" )
    val lookup_block = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "lookup_block_PrimaryTestData.csv" )
    val lookup_business_type = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "lookup_business_type_PrimaryTestData.csv" )
    val lookup_profit_centre = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "lookup_profit_centre_PrimaryTestData.csv" )
    val underwriting_block = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "underwriting_block_PrimaryTestData.csv" )

    // Load expected result into dataframe
    val expectedPolicy = utils.populateDataFrameFromCsvWithHeader( testDataOutputPath + "policy_LineStatusNotEqualC.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( policyTransformation )

    // Act //
    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    submission.registerTempTable( "submission" )
    risk.registerTempTable( "risk" )
    organisation.registerTempTable( "organisation" )
    lookup_business_type.registerTempTable( "lookup_business_type" )
    lookup_block.registerTempTable( "lookup_block" )
    lookup_profit_centre.registerTempTable( "lookup_profit_centre" )
    underwriting_block.registerTempTable( "underwriting_block" )

    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicy, result )
  }

  test( "PolicyTransformation test with null business type in line" ) {

    // Arrange //
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_NullBusinessType.csv" )
    val layer = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "layer_PrimaryTestData.csv" )
    val submission = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "submission_PrimaryTestData.csv" )
    val risk = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "risk_PrimaryTestData.csv" )
    val organisation = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "organisation_PrimaryTestData.csv" )
    val lookup_block = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "lookup_block_PrimaryTestData.csv" )
    val lookup_business_type = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "lookup_business_type_PrimaryTestData.csv" )
    val lookup_profit_centre = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "lookup_profit_centre_PrimaryTestData.csv" )
    val underwriting_block = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "underwriting_block_PrimaryTestData.csv" )

    // Load expected result into dataframe
    val expectedPolicy = utils.populateDataFrameFromCsvWithHeader( testDataOutputPath + "policy_NullBusinessTypeFromLine.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( policyTransformation )

    // Act //
    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    submission.registerTempTable( "submission" )
    risk.registerTempTable( "risk" )
    organisation.registerTempTable( "organisation" )
    lookup_business_type.registerTempTable( "lookup_business_type" )
    lookup_block.registerTempTable( "lookup_block" )
    lookup_profit_centre.registerTempTable( "lookup_profit_centre" )
    underwriting_block.registerTempTable( "underwriting_block" )

    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicy, result )
  }

  test( "Policy Transformation test with TIUK profit centre" ) {

    // Arrange //
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_TIUKProfitCentre.csv" )
    val layer = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "layer_PrimaryTestData.csv" )
    val submission = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "submission_PrimaryTestData.csv" )
    val risk = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "risk_PrimaryTestData.csv" )
    val organisation = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "organisation_PrimaryTestData.csv" )
    val lookup_block = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "lookup_block_PrimaryTestData.csv" )
    val lookup_business_type = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "lookup_business_type_PrimaryTestData.csv" )
    val lookup_profit_centre = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "lookup_profit_centre_TIUKTest.csv" )
    val underwriting_block = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "underwriting_block_PrimaryTestData.csv" )

    // Load expected result into dataframe
    val expectedPolicy = utils.populateDataFrameFromCsvWithHeader( testDataOutputPath + "policy_TIUKProfitCentre.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( policyTransformation )

    // Act //
    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    submission.registerTempTable( "submission" )
    risk.registerTempTable( "risk" )
    organisation.registerTempTable( "organisation" )
    lookup_business_type.registerTempTable( "lookup_business_type" )
    lookup_block.registerTempTable( "lookup_block" )
    lookup_profit_centre.registerTempTable( "lookup_profit_centre" )
    underwriting_block.registerTempTable( "underwriting_block" )

    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicy, result )
  }

  test( "Policy Transformation test with TIE profit centre" ) {

    // Arrange //
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_TIEProfitCentre.csv" )
    val layer = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "layer_PrimaryTestData.csv" )
    val submission = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "submission_PrimaryTestData.csv" )
    val risk = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "risk_PrimaryTestData.csv" )
    val organisation = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "organisation_PrimaryTestData.csv" )
    val lookup_block = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "lookup_block_PrimaryTestData.csv" )
    val lookup_business_type = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "lookup_business_type_PrimaryTestData.csv" )
    val lookup_profit_centre = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "lookup_profit_centre_TIETest.csv" )
    val underwriting_block = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "underwriting_block_PrimaryTestData.csv" )

    // Load expected result into dataframe
    val expectedPolicy = utils.populateDataFrameFromCsvWithHeader( testDataOutputPath + "policy_TIEProfitCentre.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( policyTransformation )

    // Act //
    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    submission.registerTempTable( "submission" )
    risk.registerTempTable( "risk" )
    organisation.registerTempTable( "organisation" )
    lookup_business_type.registerTempTable( "lookup_business_type" )
    lookup_block.registerTempTable( "lookup_block" )
    lookup_profit_centre.registerTempTable( "lookup_profit_centre" )
    underwriting_block.registerTempTable( "underwriting_block" )

    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicy, result )
  }

  test( "Policy Transformation test with Syndicate 1301 Milan profit centre" ) {

    // Arrange //
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_PrimaryTestData.csv" )
    val layer = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "layer_PrimaryTestData.csv" )
    val submission = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "submission_PrimaryTestData.csv" )
    val risk = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "risk_PrimaryTestData.csv" )
    val organisation = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "organisation_PrimaryTestData.csv" )
    val lookup_block = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "lookup_block_PrimaryTestData.csv" )
    val lookup_business_type = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "lookup_business_type_PrimaryTestData.csv" )
    val lookup_profit_centre = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "lookup_profit_centre_SyndicateBranch.csv" )
    val underwriting_block = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "underwriting_block_PrimaryTestData.csv" )

    // Load expected result into dataframe
    val expectedPolicy = utils.populateDataFrameFromCsvWithHeader( testDataOutputPath + "policy_SyndicateBranch.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( policyTransformation )

    // Act //
    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    submission.registerTempTable( "submission" )
    risk.registerTempTable( "risk" )
    organisation.registerTempTable( "organisation" )
    lookup_business_type.registerTempTable( "lookup_business_type" )
    lookup_block.registerTempTable( "lookup_block" )
    lookup_profit_centre.registerTempTable( "lookup_profit_centre" )
    underwriting_block.registerTempTable( "underwriting_block" )

    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicy, result )
  }

  test( "Policy Transformation test with null percentage fields in line" ) {

    // Arrange //
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_NullPercentageFields.csv" )
    val layer = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "layer_PrimaryTestData.csv" )
    val submission = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "submission_PrimaryTestData.csv" )
    val risk = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "risk_PrimaryTestData.csv" )
    val organisation = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "organisation_PrimaryTestData.csv" )
    val lookup_block = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "lookup_block_PrimaryTestData.csv" )
    val lookup_business_type = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "lookup_business_type_PrimaryTestData.csv" )
    val lookup_profit_centre = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "lookup_profit_centre_PrimaryTestData.csv" )
    val underwriting_block = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "underwriting_block_PrimaryTestData.csv" )

    // Load expected result into dataframe
    val expectedPolicy = utils.populateDataFrameFromCsvWithHeader( testDataOutputPath + "policy_NullPercentageFields.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( policyTransformation )

    // Act //
    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    submission.registerTempTable( "submission" )
    risk.registerTempTable( "risk" )
    organisation.registerTempTable( "organisation" )
    lookup_business_type.registerTempTable( "lookup_business_type" )
    lookup_block.registerTempTable( "lookup_block" )
    lookup_profit_centre.registerTempTable( "lookup_profit_centre" )
    underwriting_block.registerTempTable( "underwriting_block" )

    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicy, result )

  }

  test( "Policy Transformation Organisation not able to be joined with Risk" ) {

    // Arrange //
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_PrimaryTestData.csv" )
    val layer = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "layer_PrimaryTestData.csv" )
    val submission = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "submission_PrimaryTestData.csv" )
    val risk = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "risk_PrimaryTestData.csv" )
    val organisation = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "organisation_OrganisationIdNotInRisk.csv" )
    val lookup_block = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "lookup_block_PrimaryTestData.csv" )
    val lookup_business_type = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "lookup_business_type_PrimaryTestData.csv" )
    val lookup_profit_centre = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "lookup_profit_centre_PrimaryTestData.csv" )
    val underwriting_block = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "underwriting_block_PrimaryTestData.csv" )

    // Load expected result into dataframe
    val expectedPolicy = utils.populateDataFrameFromCsvWithHeader( testDataOutputPath + "policy_NullDomicileCode.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( policyTransformation )

    // Act //
    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    submission.registerTempTable( "submission" )
    risk.registerTempTable( "risk" )
    organisation.registerTempTable( "organisation" )
    lookup_business_type.registerTempTable( "lookup_business_type" )
    lookup_block.registerTempTable( "lookup_block" )
    lookup_profit_centre.registerTempTable( "lookup_profit_centre" )
    underwriting_block.registerTempTable( "underwriting_block" )

    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicy, result )

  }

}
