package com.kainos.enstar.test.TransformationUnitTesting.Policy

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ LegalEntityUtils, PolicyUtils, SQLRunner, TransformationUnitTestingUtils }
import org.apache.spark.sql.{ DataFrame, Row }
import org.scalatest.FunSuite

class ReconciliationTests extends FunSuite with DataFrameSuiteBase {

  private val testDataInputPath = "/policy/input/"
  private val schemaPath = "/policy/schemas/"

  test( "Reconciliation over test data" ) {

    // Arrange //
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframe
    val line : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( testDataInputPath + "line_ReconciliationPrimary.csv" ).toString,
      getClass.getResource( schemaPath + "line.avro" ).toString,
      _.split( "," ),
      PolicyUtils.lineMapping,
      sqlc
    )

    val layer : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( testDataInputPath + "layer_ReconciliationPrimary.csv" ).toString,
      getClass.getResource( schemaPath + "layer.avro" ).toString,
      _.split( "," ),
      PolicyUtils.layerMapping,
      sqlc
    )

    val submission : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( testDataInputPath + "submission_ReconciliationPrimary.csv" ).toString,
      getClass.getResource( schemaPath + "submission.avro" ).toString,
      _.split( "," ),
      PolicyUtils.submissionMapping,
      sqlc
    )

    val risk : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( testDataInputPath + "risk_ReconciliationPrimary.csv" ).toString,
      getClass.getResource( schemaPath + "risk.avro" ).toString,
      _.split( "," ),
      PolicyUtils.riskMapping,
      sqlc
    )

    val organisation : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( testDataInputPath + "organisation_PrimaryTestData.csv" ).toString,
      getClass.getResource( schemaPath + "organisation.avro" ).toString,
      _.split( "," ),
      PolicyUtils.organisationMapping,
      sqlc
    )

    val lookup_business_type : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( testDataInputPath + "lookup_business_type_PrimaryTestData.csv" ).toString,
      getClass.getResource( schemaPath + "lookup_business_type.avro" ).toString,
      _.split( "," ),
      PolicyUtils.lookupBusinessTypeMapping,
      sqlc
    )

    val lookup_block : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( testDataInputPath + "lookup_block_PrimaryTestData.csv" ).toString,
      getClass.getResource( schemaPath + "lookup_block.avro" ).toString,
      _.split( "," ),
      PolicyUtils.lookupBlockMapping,
      sqlc
    )

    val lookup_profit_centre : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( testDataInputPath + "lookup_profit_centre_PrimaryTestData.csv" ).toString,
      getClass.getResource( schemaPath + "lookup_profit_centre.avro" ).toString,
      _.split( "," ),
      PolicyUtils.lookupProfitCentreMapping,
      sqlc
    )

    val underwriting_block : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( testDataInputPath + "underwriting_block_PrimaryTestData.csv" ).toString,
      getClass.getResource( schemaPath + "underwriting_block.avro" ).toString,
      _.split( "," ),
      PolicyUtils.underwritingBlockMapping,
      sqlc
    )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/Policy.hql" )
    val reconStatementInput = utils.loadHQLStatementFromResource( "Reconciliation/Policy/InputRecordCount.hql" )
    val reconStatementOutput = utils.loadHQLStatementFromResource( "Reconciliation/Policy/OutputRecordCount.hql" )

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

    val output = SQLRunner.runStatement( statement, sqlc )
    output.registerTempTable( "policy" )

    val reconInput = SQLRunner.runStatement( reconStatementInput, sqlc )
    val reconOutput = SQLRunner.runStatement( reconStatementOutput, sqlc )

    // Assert //
    assertDataFrameEquals( reconInput, reconOutput )
  }

}
