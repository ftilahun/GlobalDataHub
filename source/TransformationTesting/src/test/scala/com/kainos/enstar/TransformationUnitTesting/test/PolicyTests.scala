package com.kainos.enstar.TransformationUnitTesting.test

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ PolicyUtils, SQLRunner, TransformationUnitTestingUtils }
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

/**
 * Created by caoimheb on 23/11/2016.
 */
class PolicyTests extends FunSuite with DataFrameSuiteBase {

  test( "Policy Transformation mapping happy path data" ){

    // Arrange //
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/line_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/line.avro" ).toString,
      PolicyUtils.lineMapping,
      sqlc
    )

    val layer : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/layer_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/layer.avro" ).toString,
      PolicyUtils.layerMapping,
      sqlc
    )

    val submission : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/submission_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/submission.avro" ).toString,
      PolicyUtils.submissionMapping,
      sqlc
    )

    val risk : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/risk_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/risk.avro" ).toString,
      PolicyUtils.riskMapping,
      sqlc
    )

    val organisation : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/organisation_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/organisation.avro" ).toString,
      PolicyUtils.organisationMapping,
      sqlc
    )

    val lookup_business_type : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/lookup_business_type_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/lookup_business_type.avro" ).toString,
      PolicyUtils.lookupBusinessTypeMapping,
      sqlc
    )

    val lookup_block : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/lookup_block_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/lookup_block.avro" ).toString,
      PolicyUtils.lookupBlockMapping,
      sqlc
    )

    val lookup_profit_centre : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/lookup_profit_centre_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/lookup_profit_centre.avro" ).toString,
      PolicyUtils.lookupProfitCentreMapping,
      sqlc
    )

    val underwriting_block : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/underwriting_block_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/underwriting_block.avro" ).toString,
      PolicyUtils.underwritingBlockMapping,
      sqlc
    )

    // Load expected result into dataframe
    val expectedPolicy : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/output/policy_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/policy.avro" ).toString,
      PolicyUtils.policyMapping,
      sqlc
    )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Policy.hql" )

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

  test( "PolicyTransformation test with null inception date in layer" ){

    // Arrange //
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val layer : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/layer_test2.csv" ).toString,
      getClass.getResource( "/policy/schemas/layer.avro" ).toString,
      PolicyUtils.layerMapping,
      sqlc
    )

    val line : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/line_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/line.avro" ).toString,
      PolicyUtils.lineMapping,
      sqlc
    )

    val submission : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/submission_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/submission.avro" ).toString,
      PolicyUtils.submissionMapping,
      sqlc
    )

    val risk : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/risk_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/risk.avro" ).toString,
      PolicyUtils.riskMapping,
      sqlc
    )

    val organisation : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/organisation_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/organisation.avro" ).toString,
      PolicyUtils.organisationMapping,
      sqlc
    )

    val lookup_business_type : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/lookup_business_type_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/lookup_business_type.avro" ).toString,
      PolicyUtils.lookupBusinessTypeMapping,
      sqlc
    )

    val lookup_block : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/lookup_block_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/lookup_block.avro" ).toString,
      PolicyUtils.lookupBlockMapping,
      sqlc
    )

    val lookup_profit_centre : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/lookup_profit_centre_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/lookup_profit_centre.avro" ).toString,
      PolicyUtils.lookupProfitCentreMapping,
      sqlc
    )

    val underwriting_block : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/underwriting_block_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/underwriting_block.avro" ).toString,
      PolicyUtils.underwritingBlockMapping,
      sqlc
    )

    // Load expected result into dataframe
    val expectedPolicy : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/output/policy_test2.csv" ).toString,
      getClass.getResource( "/policy/schemas/policy.avro" ).toString,
      PolicyUtils.policyMapping,
      sqlc
    )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Policy.hql" )

    // Act //
    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    submission.registerTempTable( "submission" )
    risk.registerTempTable( "risk" )
    organisation.registerTempTable( "organisation" )
    lookup_business_type.registerTempTable( "lookup_business_type" )
    lookup_block.registerTempTable( "lookup_block" )
    lookup_profit_centre.registerTempTable( "lookup_profit_centre" )

    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicy, result )
  }

  test( "PolicyTransformation test with null expiry date in layer" ){

    // Arrange //
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val layer : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/layer_test3.csv" ).toString,
      getClass.getResource( "/policy/schemas/layer.avro" ).toString,
      PolicyUtils.layerMapping,
      sqlc
    )

    val line : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/line_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/line.avro" ).toString,
      PolicyUtils.lineMapping,
      sqlc
    )

    val submission : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/submission_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/submission.avro" ).toString,
      PolicyUtils.submissionMapping,
      sqlc
    )

    val risk : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/risk_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/risk.avro" ).toString,
      PolicyUtils.riskMapping,
      sqlc
    )

    val organisation : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/organisation_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/organisation.avro" ).toString,
      PolicyUtils.organisationMapping,
      sqlc
    )

    val lookup_business_type : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/lookup_business_type_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/lookup_business_type.avro" ).toString,
      PolicyUtils.lookupBusinessTypeMapping,
      sqlc
    )

    val lookup_block : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/lookup_block_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/lookup_block.avro" ).toString,
      PolicyUtils.lookupBlockMapping,
      sqlc
    )

    val lookup_profit_centre : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/lookup_profit_centre_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/lookup_profit_centre.avro" ).toString,
      PolicyUtils.lookupProfitCentreMapping,
      sqlc
    )

    val underwriting_block : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/underwriting_block_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/underwriting_block.avro" ).toString,
      PolicyUtils.underwritingBlockMapping,
      sqlc
    )

    // Load expected result into dataframe
    val expectedPolicy : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/output/policy_test3.csv" ).toString,
      getClass.getResource( "/policy/schemas/policy.avro" ).toString,
      PolicyUtils.policyMapping,
      sqlc
    )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Policy.hql" )

    // Act //
    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    submission.registerTempTable( "submission" )
    risk.registerTempTable( "risk" )
    organisation.registerTempTable( "organisation" )
    lookup_business_type.registerTempTable( "lookup_business_type" )
    lookup_block.registerTempTable( "lookup_block" )
    lookup_profit_centre.registerTempTable( "lookup_profit_centre" )

    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicy, result )
  }

  test( "PolicyTransformation test with null values in layer inception date, expiry date, filcode, unique market ref" ){

    // Arrange //
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val layer : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/layer_test4.csv" ).toString,
      getClass.getResource( "/policy/schemas/layer.avro" ).toString,
      PolicyUtils.layerMapping,
      sqlc
    )

    val line : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/line_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/line.avro" ).toString,
      PolicyUtils.lineMapping,
      sqlc
    )

    val submission : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/submission_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/submission.avro" ).toString,
      PolicyUtils.submissionMapping,
      sqlc
    )

    val risk : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/risk_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/risk.avro" ).toString,
      PolicyUtils.riskMapping,
      sqlc
    )

    val organisation : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/organisation_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/organisation.avro" ).toString,
      PolicyUtils.organisationMapping,
      sqlc
    )

    val lookup_business_type : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/lookup_business_type_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/lookup_business_type.avro" ).toString,
      PolicyUtils.lookupBusinessTypeMapping,
      sqlc
    )

    val lookup_block : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/lookup_block_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/lookup_block.avro" ).toString,
      PolicyUtils.lookupBlockMapping,
      sqlc
    )

    val lookup_profit_centre : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/lookup_profit_centre_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/lookup_profit_centre.avro" ).toString,
      PolicyUtils.lookupProfitCentreMapping,
      sqlc
    )

    val underwriting_block : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/underwriting_block_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/underwriting_block.avro" ).toString,
      PolicyUtils.underwritingBlockMapping,
      sqlc
    )

    // Load expected result into dataframe
    val expectedPolicy : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/output/policy_test4.csv" ).toString,
      getClass.getResource( "/policy/schemas/policy.avro" ).toString,
      PolicyUtils.policyMapping,
      sqlc
    )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Policy.hql" )

    // Act //
    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    submission.registerTempTable( "submission" )
    risk.registerTempTable( "risk" )
    organisation.registerTempTable( "organisation" )
    lookup_business_type.registerTempTable( "lookup_business_type" )
    lookup_block.registerTempTable( "lookup_block" )
    lookup_profit_centre.registerTempTable( "lookup_profit_centre" )

    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicy, result )
  }

  test( "PolicyTransformation test with no link between line and lookup profit centre" ){

    // Arrange //
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val layer : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/layer_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/layer.avro" ).toString,
      PolicyUtils.layerMapping,
      sqlc
    )

    val line : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/line_test5.csv" ).toString,
      getClass.getResource( "/policy/schemas/line.avro" ).toString,
      PolicyUtils.lineMapping,
      sqlc
    )

    val submission : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/submission_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/submission.avro" ).toString,
      PolicyUtils.submissionMapping,
      sqlc
    )

    val risk : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/risk_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/risk.avro" ).toString,
      PolicyUtils.riskMapping,
      sqlc
    )

    val organisation : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/organisation_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/organisation.avro" ).toString,
      PolicyUtils.organisationMapping,
      sqlc
    )

    val lookup_business_type : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/lookup_business_type_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/lookup_business_type.avro" ).toString,
      PolicyUtils.lookupBusinessTypeMapping,
      sqlc
    )

    val lookup_block : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/lookup_block_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/lookup_block.avro" ).toString,
      PolicyUtils.lookupBlockMapping,
      sqlc
    )

    val lookup_profit_centre : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/lookup_profit_centre_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/lookup_profit_centre.avro" ).toString,
      PolicyUtils.lookupProfitCentreMapping,
      sqlc
    )

    val underwriting_block : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/underwriting_block_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/underwriting_block.avro" ).toString,
      PolicyUtils.underwritingBlockMapping,
      sqlc
    )

    // Load expected result into dataframe
    val expectedPolicy : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/output/policy_test5.csv" ).toString,
      getClass.getResource( "/policy/schemas/policy.avro" ).toString,
      PolicyUtils.policyMapping,
      sqlc
    )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Policy.hql" )

    // Act //
    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    submission.registerTempTable( "submission" )
    risk.registerTempTable( "risk" )
    organisation.registerTempTable( "organisation" )
    lookup_business_type.registerTempTable( "lookup_business_type" )
    lookup_block.registerTempTable( "lookup_block" )
    lookup_profit_centre.registerTempTable( "lookup_profit_centre" )

    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicy, result )
  }

  test( "PolicyTransformation test with null block in line" ){

    // Arrange //
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val layer : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/layer_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/layer.avro" ).toString,
      PolicyUtils.layerMapping,
      sqlc
    )

    val line : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/line_test6.csv" ).toString,
      getClass.getResource( "/policy/schemas/line.avro" ).toString,
      PolicyUtils.lineMapping,
      sqlc
    )

    val submission : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/submission_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/submission.avro" ).toString,
      PolicyUtils.submissionMapping,
      sqlc
    )

    val risk : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/risk_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/risk.avro" ).toString,
      PolicyUtils.riskMapping,
      sqlc
    )

    val organisation : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/organisation_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/organisation.avro" ).toString,
      PolicyUtils.organisationMapping,
      sqlc
    )

    val lookup_business_type : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/lookup_business_type_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/lookup_business_type.avro" ).toString,
      PolicyUtils.lookupBusinessTypeMapping,
      sqlc
    )

    val lookup_block : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/lookup_block_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/lookup_block.avro" ).toString,
      PolicyUtils.lookupBlockMapping,
      sqlc
    )

    val lookup_profit_centre : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/lookup_profit_centre_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/lookup_profit_centre.avro" ).toString,
      PolicyUtils.lookupProfitCentreMapping,
      sqlc
    )

    val underwriting_block : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/underwriting_block_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/underwriting_block.avro" ).toString,
      PolicyUtils.underwritingBlockMapping,
      sqlc
    )

    // Load expected result into dataframe
    val expectedPolicy : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/output/policy_test6.csv" ).toString,
      getClass.getResource( "/policy/schemas/policy.avro" ).toString,
      PolicyUtils.policyMapping,
      sqlc
    )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Policy.hql" )

    // Act //
    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    submission.registerTempTable( "submission" )
    risk.registerTempTable( "risk" )
    organisation.registerTempTable( "organisation" )
    lookup_business_type.registerTempTable( "lookup_business_type" )
    lookup_block.registerTempTable( "lookup_block" )
    lookup_profit_centre.registerTempTable( "lookup_profit_centre" )

    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicy, result )
  }

  test( "PolicyTransformation test with line status not equal to C" ){

    // Arrange //
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val layer : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/layer_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/layer.avro" ).toString,
      PolicyUtils.layerMapping,
      sqlc
    )

    val line : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/line_test7.csv" ).toString,
      getClass.getResource( "/policy/schemas/line.avro" ).toString,
      PolicyUtils.lineMapping,
      sqlc
    )

    val submission : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/submission_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/submission.avro" ).toString,
      PolicyUtils.submissionMapping,
      sqlc
    )

    val risk : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/risk_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/risk.avro" ).toString,
      PolicyUtils.riskMapping,
      sqlc
    )

    val organisation : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/organisation_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/organisation.avro" ).toString,
      PolicyUtils.organisationMapping,
      sqlc
    )

    val lookup_business_type : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/lookup_business_type_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/lookup_business_type.avro" ).toString,
      PolicyUtils.lookupBusinessTypeMapping,
      sqlc
    )

    val lookup_block : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/lookup_block_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/lookup_block.avro" ).toString,
      PolicyUtils.lookupBlockMapping,
      sqlc
    )

    val lookup_profit_centre : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/lookup_profit_centre_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/lookup_profit_centre.avro" ).toString,
      PolicyUtils.lookupProfitCentreMapping,
      sqlc
    )

    val underwriting_block : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/underwriting_block_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/underwriting_block.avro" ).toString,
      PolicyUtils.underwritingBlockMapping,
      sqlc
    )

    // Load expected result into dataframe
    val expectedPolicy : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/output/policy_test7.csv" ).toString,
      getClass.getResource( "/policy/schemas/policy.avro" ).toString,
      PolicyUtils.policyMapping,
      sqlc
    )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Policy.hql" )

    // Act //
    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    submission.registerTempTable( "submission" )
    risk.registerTempTable( "risk" )
    organisation.registerTempTable( "organisation" )
    lookup_business_type.registerTempTable( "lookup_business_type" )
    lookup_block.registerTempTable( "lookup_block" )
    lookup_profit_centre.registerTempTable( "lookup_profit_centre" )

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
    val line : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/line_test8.csv" ).toString,
      getClass.getResource( "/policy/schemas/line.avro" ).toString,
      PolicyUtils.lineMapping,
      sqlc
    )

    val layer : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/layer_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/layer.avro" ).toString,
      PolicyUtils.layerMapping,
      sqlc
    )

    val submission : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/submission_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/submission.avro" ).toString,
      PolicyUtils.submissionMapping,
      sqlc
    )

    val risk : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/risk_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/risk.avro" ).toString,
      PolicyUtils.riskMapping,
      sqlc
    )

    val organisation : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/organisation_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/organisation.avro" ).toString,
      PolicyUtils.organisationMapping,
      sqlc
    )

    val lookup_business_type : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/lookup_business_type_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/lookup_business_type.avro" ).toString,
      PolicyUtils.lookupBusinessTypeMapping,
      sqlc
    )

    val lookup_block : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/lookup_block_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/lookup_block.avro" ).toString,
      PolicyUtils.lookupBlockMapping,
      sqlc
    )

    val lookup_profit_centre : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/lookup_profit_centre_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/lookup_profit_centre.avro" ).toString,
      PolicyUtils.lookupProfitCentreMapping,
      sqlc
    )

    val underwriting_block : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/underwriting_block_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/underwriting_block.avro" ).toString,
      PolicyUtils.underwritingBlockMapping,
      sqlc
    )

    // Load expected result into dataframe
    val expectedPolicy : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/output/policy_test8.csv" ).toString,
      getClass.getResource( "/policy/schemas/policy.avro" ).toString,
      PolicyUtils.policyMapping,
      sqlc
    )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Policy.hql" )

    // Act //
    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    submission.registerTempTable( "submission" )
    risk.registerTempTable( "risk" )
    organisation.registerTempTable( "organisation" )
    lookup_business_type.registerTempTable( "lookup_business_type" )
    lookup_block.registerTempTable( "lookup_block" )
    lookup_profit_centre.registerTempTable( "lookup_profit_centre" )

    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicy, result )
  }

}
