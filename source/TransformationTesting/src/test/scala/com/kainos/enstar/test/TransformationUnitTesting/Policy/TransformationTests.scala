package com.kainos.enstar.test.TransformationUnitTesting.Policy

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ PolicyUtils, SQLRunner, TransformationUnitTestingUtils }
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.scalatest.FunSuite

/**
 * Created by caoimheb on 23/11/2016.
 */
class TransformationTests extends FunSuite with DataFrameSuiteBase {

  private val utils = new TransformationUnitTestingUtils

  def populateDataFrameWithLineTestData( dataFileName : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/" + dataFileName ).toString,
      getClass.getResource( "/policy/schemas/line.avro" ).toString,
      _.split( "," ),
      PolicyUtils.lineMapping,
      sqlc
    )
  }

  def populateDataFrameWithLayerTestData( dataFileName : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/" + dataFileName ).toString,
      getClass.getResource( "/policy/schemas/layer.avro" ).toString,
      _.split( "," ),
      PolicyUtils.layerMapping,
      sqlc
    )
  }

  def populateDataFrameWithSubmissionTestData( dataFileName : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/" + dataFileName ).toString,
      getClass.getResource( "/policy/schemas/submission.avro" ).toString,
      _.split( "," ),
      PolicyUtils.submissionMapping,
      sqlc
    )
  }

  def populateDataFrameWithRiskTestData( dataFileName : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/" + dataFileName ).toString,
      getClass.getResource( "/policy/schemas/risk.avro" ).toString,
      _.split( "," ),
      PolicyUtils.riskMapping,
      sqlc
    )
  }

  def populateDataFrameWithLookupBlockTestData( dataFileName : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/" + dataFileName ).toString,
      getClass.getResource( "/policy/schemas/lookup_block.avro" ).toString,
      _.split( "," ),
      PolicyUtils.lookupBlockMapping,
      sqlc
    )
  }

  def populateDataFrameWithLookupProfitCentreTestData( dataFileName : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/" + dataFileName ).toString,
      getClass.getResource( "/policy/schemas/lookup_profit_centre.avro" ).toString,
      _.split( "," ),
      PolicyUtils.lookupProfitCentreMapping,
      sqlc
    )
  }

  def populateDataFrameWithLookupBusinessTypeTestData( dataFileName : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/" + dataFileName ).toString,
      getClass.getResource( "/policy/schemas/lookup_business_type.avro" ).toString,
      _.split( "," ),
      PolicyUtils.lookupBusinessTypeMapping,
      sqlc
    )
  }

  def populateDataFrameWithOrganisationTestData( dataFileName : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/" + dataFileName ).toString,
      getClass.getResource( "/policy/schemas/organisation.avro" ).toString,
      _.split( "," ),
      PolicyUtils.organisationMapping,
      sqlc
    )
  }

  def populateDataFrameWithUnderwritingBlockTestData( dataFileName : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/" + dataFileName ).toString,
      getClass.getResource( "/policy/schemas/underwriting_block.avro" ).toString,
      _.split( "," ),
      PolicyUtils.underwritingBlockMapping,
      sqlc
    )
  }

  def populateDataFrameWithPolicyTestData( dataFileName : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/output/" + dataFileName ).toString,
      getClass.getResource( "/policy/schemas/policy.avro" ).toString,
      _.split( "," ),
      PolicyUtils.policyMapping,
      sqlc
    )
  }

  test( "Policy Transformation mapping happy path data" ) {

    // Arrange //
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line = this.populateDataFrameWithLineTestData( "line_PrimaryTestData.csv", sqlc )
    val layer = this.populateDataFrameWithLayerTestData( "layer_PrimaryTestData.csv", sqlc )
    val submission = this.populateDataFrameWithSubmissionTestData( "submission_PrimaryTestData.csv", sqlc )
    val risk = this.populateDataFrameWithRiskTestData( "risk_PrimaryTestData.csv", sqlc )
    val organisation = this.populateDataFrameWithOrganisationTestData( "organisation_PrimaryTestData.csv", sqlc )
    val lookup_block = this.populateDataFrameWithLookupBlockTestData( "lookup_block_PrimaryTestData.csv", sqlc )
    val lookup_business_type = this.populateDataFrameWithLookupBusinessTypeTestData( "lookup_business_type_PrimaryTestData.csv", sqlc )
    val lookup_profit_centre = this.populateDataFrameWithLookupProfitCentreTestData( "lookup_profit_centre_PrimaryTestData.csv", sqlc )
    val underwriting_block = this.populateDataFrameWithUnderwritingBlockTestData( "underwriting_block_PrimaryTestData.csv", sqlc )

    // Load expected result into dataframe
    val expectedPolicy = this.populateDataFrameWithPolicyTestData( "policy_PrimaryTestData.csv", sqlc )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/Policy.hql" )

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
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line = this.populateDataFrameWithLineTestData( "line_PrimaryTestData.csv", sqlc )
    val layer = this.populateDataFrameWithLayerTestData( "layer_NullInceptionDate.csv", sqlc )
    val submission = this.populateDataFrameWithSubmissionTestData( "submission_PrimaryTestData.csv", sqlc )
    val risk = this.populateDataFrameWithRiskTestData( "risk_PrimaryTestData.csv", sqlc )
    val organisation = this.populateDataFrameWithOrganisationTestData( "organisation_PrimaryTestData.csv", sqlc )
    val lookup_block = this.populateDataFrameWithLookupBlockTestData( "lookup_block_PrimaryTestData.csv", sqlc )
    val lookup_business_type = this.populateDataFrameWithLookupBusinessTypeTestData( "lookup_business_type_PrimaryTestData.csv", sqlc )
    val lookup_profit_centre = this.populateDataFrameWithLookupProfitCentreTestData( "lookup_profit_centre_PrimaryTestData.csv", sqlc )
    val underwriting_block = this.populateDataFrameWithUnderwritingBlockTestData( "underwriting_block_PrimaryTestData.csv", sqlc )

    // Load expected result into dataframe
    val expectedPolicy = this.populateDataFrameWithPolicyTestData( "policy_NullInceptionDateInLayer.csv", sqlc )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/Policy.hql" )

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
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line = this.populateDataFrameWithLineTestData( "line_PrimaryTestData.csv", sqlc )
    val layer = this.populateDataFrameWithLayerTestData( "layer_NullExpiryDate.csv", sqlc )
    val submission = this.populateDataFrameWithSubmissionTestData( "submission_PrimaryTestData.csv", sqlc )
    val risk = this.populateDataFrameWithRiskTestData( "risk_PrimaryTestData.csv", sqlc )
    val organisation = this.populateDataFrameWithOrganisationTestData( "organisation_PrimaryTestData.csv", sqlc )
    val lookup_block = this.populateDataFrameWithLookupBlockTestData( "lookup_block_PrimaryTestData.csv", sqlc )
    val lookup_business_type = this.populateDataFrameWithLookupBusinessTypeTestData( "lookup_business_type_PrimaryTestData.csv", sqlc )
    val lookup_profit_centre = this.populateDataFrameWithLookupProfitCentreTestData( "lookup_profit_centre_PrimaryTestData.csv", sqlc )
    val underwriting_block = this.populateDataFrameWithUnderwritingBlockTestData( "underwriting_block_PrimaryTestData.csv", sqlc )

    // Load expected result into dataframe
    val expectedPolicy = this.populateDataFrameWithPolicyTestData( "policy_NullExpiryDateInLayer.csv", sqlc )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/Policy.hql" )

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
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line = this.populateDataFrameWithLineTestData( "line_PrimaryTestData.csv", sqlc )
    val layer = this.populateDataFrameWithLayerTestData( "layer_VariousNullValues.csv", sqlc )
    val submission = this.populateDataFrameWithSubmissionTestData( "submission_PrimaryTestData.csv", sqlc )
    val risk = this.populateDataFrameWithRiskTestData( "risk_PrimaryTestData.csv", sqlc )
    val organisation = this.populateDataFrameWithOrganisationTestData( "organisation_PrimaryTestData.csv", sqlc )
    val lookup_block = this.populateDataFrameWithLookupBlockTestData( "lookup_block_PrimaryTestData.csv", sqlc )
    val lookup_business_type = this.populateDataFrameWithLookupBusinessTypeTestData( "lookup_business_type_PrimaryTestData.csv", sqlc )
    val lookup_profit_centre = this.populateDataFrameWithLookupProfitCentreTestData( "lookup_profit_centre_PrimaryTestData.csv", sqlc )
    val underwriting_block = this.populateDataFrameWithUnderwritingBlockTestData( "underwriting_block_PrimaryTestData.csv", sqlc )

    // Load expected result into dataframe
    val expectedPolicy = this.populateDataFrameWithPolicyTestData( "policy_VariousNullsInLayer.csv", sqlc )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/Policy.hql" )

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
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line = this.populateDataFrameWithLineTestData( "line_NullProfitCentreCode.csv", sqlc )
    val layer = this.populateDataFrameWithLayerTestData( "layer_PrimaryTestData.csv", sqlc )
    val submission = this.populateDataFrameWithSubmissionTestData( "submission_PrimaryTestData.csv", sqlc )
    val risk = this.populateDataFrameWithRiskTestData( "risk_PrimaryTestData.csv", sqlc )
    val organisation = this.populateDataFrameWithOrganisationTestData( "organisation_PrimaryTestData.csv", sqlc )
    val lookup_block = this.populateDataFrameWithLookupBlockTestData( "lookup_block_PrimaryTestData.csv", sqlc )
    val lookup_business_type = this.populateDataFrameWithLookupBusinessTypeTestData( "lookup_business_type_PrimaryTestData.csv", sqlc )
    val lookup_profit_centre = this.populateDataFrameWithLookupProfitCentreTestData( "lookup_profit_centre_PrimaryTestData.csv", sqlc )
    val underwriting_block = this.populateDataFrameWithUnderwritingBlockTestData( "underwriting_block_PrimaryTestData.csv", sqlc )

    // Load expected result into dataframe
    val expectedPolicy = this.populateDataFrameWithPolicyTestData( "policy_NullProfitCentreCodeInLine.csv", sqlc )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/Policy.hql" )

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
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line = this.populateDataFrameWithLineTestData( "line_NullBlock.csv", sqlc )
    val layer = this.populateDataFrameWithLayerTestData( "layer_PrimaryTestData.csv", sqlc )
    val submission = this.populateDataFrameWithSubmissionTestData( "submission_PrimaryTestData.csv", sqlc )
    val risk = this.populateDataFrameWithRiskTestData( "risk_PrimaryTestData.csv", sqlc )
    val organisation = this.populateDataFrameWithOrganisationTestData( "organisation_PrimaryTestData.csv", sqlc )
    val lookup_block = this.populateDataFrameWithLookupBlockTestData( "lookup_block_PrimaryTestData.csv", sqlc )
    val lookup_business_type = this.populateDataFrameWithLookupBusinessTypeTestData( "lookup_business_type_PrimaryTestData.csv", sqlc )
    val lookup_profit_centre = this.populateDataFrameWithLookupProfitCentreTestData( "lookup_profit_centre_PrimaryTestData.csv", sqlc )
    val underwriting_block = this.populateDataFrameWithUnderwritingBlockTestData( "underwriting_block_PrimaryTestData.csv", sqlc )

    // Load expected result into dataframe
    val expectedPolicy = this.populateDataFrameWithPolicyTestData( "policy_NullBlockInLine.csv", sqlc )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/Policy.hql" )

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
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line = this.populateDataFrameWithLineTestData( "line_LineStatusNotC.csv", sqlc )
    val layer = this.populateDataFrameWithLayerTestData( "layer_PrimaryTestData.csv", sqlc )
    val submission = this.populateDataFrameWithSubmissionTestData( "submission_PrimaryTestData.csv", sqlc )
    val risk = this.populateDataFrameWithRiskTestData( "risk_PrimaryTestData.csv", sqlc )
    val organisation = this.populateDataFrameWithOrganisationTestData( "organisation_PrimaryTestData.csv", sqlc )
    val lookup_block = this.populateDataFrameWithLookupBlockTestData( "lookup_block_PrimaryTestData.csv", sqlc )
    val lookup_business_type = this.populateDataFrameWithLookupBusinessTypeTestData( "lookup_business_type_PrimaryTestData.csv", sqlc )
    val lookup_profit_centre = this.populateDataFrameWithLookupProfitCentreTestData( "lookup_profit_centre_PrimaryTestData.csv", sqlc )
    val underwriting_block = this.populateDataFrameWithUnderwritingBlockTestData( "underwriting_block_PrimaryTestData.csv", sqlc )

    // Load expected result into dataframe
    val expectedPolicy = this.populateDataFrameWithPolicyTestData( "policy_LineStatusNotEqualC.csv", sqlc )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/Policy.hql" )

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
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line = this.populateDataFrameWithLineTestData( "line_NullBusinessType.csv", sqlc )
    val layer = this.populateDataFrameWithLayerTestData( "layer_PrimaryTestData.csv", sqlc )
    val submission = this.populateDataFrameWithSubmissionTestData( "submission_PrimaryTestData.csv", sqlc )
    val risk = this.populateDataFrameWithRiskTestData( "risk_PrimaryTestData.csv", sqlc )
    val organisation = this.populateDataFrameWithOrganisationTestData( "organisation_PrimaryTestData.csv", sqlc )
    val lookup_block = this.populateDataFrameWithLookupBlockTestData( "lookup_block_PrimaryTestData.csv", sqlc )
    val lookup_business_type = this.populateDataFrameWithLookupBusinessTypeTestData( "lookup_business_type_PrimaryTestData.csv", sqlc )
    val lookup_profit_centre = this.populateDataFrameWithLookupProfitCentreTestData( "lookup_profit_centre_PrimaryTestData.csv", sqlc )
    val underwriting_block = this.populateDataFrameWithUnderwritingBlockTestData( "underwriting_block_PrimaryTestData.csv", sqlc )

    // Load expected result into dataframe
    val expectedPolicy = this.populateDataFrameWithPolicyTestData( "policy_NullBusineTypeFromLine.csv", sqlc )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/Policy.hql" )

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

  test( "Policy Transformation test with TIUK profit centre" ){
    // Arrange //
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line = this.populateDataFrameWithLineTestData( "line_TIUKProfitCentre.csv", sqlc )
    val layer = this.populateDataFrameWithLayerTestData( "layer_PrimaryTestData.csv", sqlc )
    val submission = this.populateDataFrameWithSubmissionTestData( "submission_PrimaryTestData.csv", sqlc )
    val risk = this.populateDataFrameWithRiskTestData( "risk_PrimaryTestData.csv", sqlc )
    val organisation = this.populateDataFrameWithOrganisationTestData( "organisation_PrimaryTestData.csv", sqlc )
    val lookup_block = this.populateDataFrameWithLookupBlockTestData( "lookup_block_PrimaryTestData.csv", sqlc )
    val lookup_business_type = this.populateDataFrameWithLookupBusinessTypeTestData( "lookup_business_type_PrimaryTestData.csv", sqlc )
    val lookup_profit_centre = this.populateDataFrameWithLookupProfitCentreTestData( "lookup_profit_centre_TIUKTest.csv", sqlc )
    val underwriting_block = this.populateDataFrameWithUnderwritingBlockTestData( "underwriting_block_PrimaryTestData.csv", sqlc )

    // Load expected result into dataframe
    val expectedPolicy = this.populateDataFrameWithPolicyTestData( "policy_TIUKProfitCentre.csv", sqlc )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/Policy.hql" )

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

  test( "Policy Transformation test with TIE profit centre" ){
    // Arrange //
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line = this.populateDataFrameWithLineTestData( "line_TIEProfitCentre.csv", sqlc )
    val layer = this.populateDataFrameWithLayerTestData( "layer_PrimaryTestData.csv", sqlc )
    val submission = this.populateDataFrameWithSubmissionTestData( "submission_PrimaryTestData.csv", sqlc )
    val risk = this.populateDataFrameWithRiskTestData( "risk_PrimaryTestData.csv", sqlc )
    val organisation = this.populateDataFrameWithOrganisationTestData( "organisation_PrimaryTestData.csv", sqlc )
    val lookup_block = this.populateDataFrameWithLookupBlockTestData( "lookup_block_PrimaryTestData.csv", sqlc )
    val lookup_business_type = this.populateDataFrameWithLookupBusinessTypeTestData( "lookup_business_type_PrimaryTestData.csv", sqlc )
    val lookup_profit_centre = this.populateDataFrameWithLookupProfitCentreTestData( "lookup_profit_centre_TIETest.csv", sqlc )
    val underwriting_block = this.populateDataFrameWithUnderwritingBlockTestData( "underwriting_block_PrimaryTestData.csv", sqlc )

    // Load expected result into dataframe
    val expectedPolicy = this.populateDataFrameWithPolicyTestData( "policy_TIEProfitCentre.csv", sqlc )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/Policy.hql" )

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

  test( "Policy Transformation test with Syndicate 1301 Milan profit centre" ){
    // Arrange //
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line = this.populateDataFrameWithLineTestData( "line_PrimaryTestData.csv", sqlc )
    val layer = this.populateDataFrameWithLayerTestData( "layer_PrimaryTestData.csv", sqlc )
    val submission = this.populateDataFrameWithSubmissionTestData( "submission_PrimaryTestData.csv", sqlc )
    val risk = this.populateDataFrameWithRiskTestData( "risk_PrimaryTestData.csv", sqlc )
    val organisation = this.populateDataFrameWithOrganisationTestData( "organisation_PrimaryTestData.csv", sqlc )
    val lookup_block = this.populateDataFrameWithLookupBlockTestData( "lookup_block_PrimaryTestData.csv", sqlc )
    val lookup_business_type = this.populateDataFrameWithLookupBusinessTypeTestData( "lookup_business_type_PrimaryTestData.csv", sqlc )
    val lookup_profit_centre = this.populateDataFrameWithLookupProfitCentreTestData( "lookup_profit_centre_SyndicateBranch.csv", sqlc )
    val underwriting_block = this.populateDataFrameWithUnderwritingBlockTestData( "underwriting_block_PrimaryTestData.csv", sqlc )

    // Load expected result into dataframe
    val expectedPolicy = this.populateDataFrameWithPolicyTestData( "policy_SyndicateBranch.csv", sqlc )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/Policy.hql" )

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
