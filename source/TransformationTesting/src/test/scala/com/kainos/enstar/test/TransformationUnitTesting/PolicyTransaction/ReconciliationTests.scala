package com.kainos.enstar.test.TransformationUnitTesting.PolicyTransaction

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, TransformationUnitTestingUtils }
import org.scalatest.FunSuite

class ReconciliationTests extends FunSuite with DataFrameSuiteBase {

  test( "PolicyTransaction - Reconciliation over test data" ) {

    // Arrange //
    implicit val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils
    val testDataInputPath = "/policytransaction_writtenpremium/input/"

    val layer = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "layer_PrimaryTestData.csv" )
    val layer_trust_fund = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "layer_trust_fund_PrimaryTestData.csv" )
    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_PrimaryTestData.csv" )
    val line_risk_code = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_risk_code_PrimaryTestData.csv" )

    // Load the hql statements under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/PolicyTransaction_WrittenPremium.hql" )
    val reconStatementInput = utils.loadHQLStatementFromResource( "Reconciliation/PolicyTransaction/InputRecordCount.hql" )
    val reconStatementOutput = utils.loadHQLStatementFromResource( "Reconciliation/PolicyTransaction/OutputRecordCount.hql" )

    // Act //
    layer.registerTempTable( "layer" )
    layer_trust_fund.registerTempTable( "layer_trust_fund" )
    line.registerTempTable( "line" )
    line_risk_code.registerTempTable( "line_risk_code" )

    val output = SQLRunner.runStatement( statement, sqlc )
    output.registerTempTable( "policytransaction" )

    val reconInput = SQLRunner.runStatement( reconStatementInput, sqlc )
    val reconOutput = SQLRunner.runStatement( reconStatementOutput, sqlc )

    // Assert //
    assertDataFrameEquals( reconInput, reconOutput )
  }

}
