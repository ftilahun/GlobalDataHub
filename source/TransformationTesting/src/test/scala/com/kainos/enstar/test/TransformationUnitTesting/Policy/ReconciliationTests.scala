package com.kainos.enstar.test.TransformationUnitTesting.Policy

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, TransformationUnitTestingUtils }
import org.scalatest.FunSuite

class ReconciliationTests extends FunSuite with DataFrameSuiteBase {

  private val testDataInputPath = "/ndex/policy/input/"

  test( "Reconciliation over test data" ) {

    // Arrange //
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframe
    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_ReconciliationPrimary.csv" )
    val layer = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "layer_ReconciliationPrimary.csv" )
    val submission = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "submission_ReconciliationPrimary.csv" )
    val risk = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "risk_ReconciliationPrimary.csv" )
    val organisation = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "PrimaryTestData.csv" )
    val lookup_block = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "PrimaryTestData.csv" )
    val lookup_business_type = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "PrimaryTestData.csv" )
    val lookup_profit_centre = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "PrimaryTestData.csv" )
    val underwriting_block = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "PrimaryTestData.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/ndex/Policy.hql" )
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
