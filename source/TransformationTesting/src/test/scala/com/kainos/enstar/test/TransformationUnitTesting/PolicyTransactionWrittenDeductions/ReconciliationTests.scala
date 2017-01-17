package com.kainos.enstar.test.TransformationUnitTesting.PolicyTransactionWrittenDeductions

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.transformation.udf.NetAsPctOfGross
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, TransformationUnitTestingUtils }
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

class ReconciliationTests extends FunSuite with DataFrameSuiteBase {

  test( "Reconciliation over test data" ) {

    // Arrange //
    // Use sqlContext from spark-testing-base
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )
    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframe
    val layer : DataFrame = utils.populateDataFrameFromCsvWithHeader( "/policytransaction_writtendeductions/input/layer_PrimaryTestData.csv" )
    val layer_deduction : DataFrame = utils.populateDataFrameFromCsvWithHeader( "/policytransaction_writtendeductions/input/layer_deduction_PrimaryTestData.csv" )
    val layer_trust_fund : DataFrame = utils.populateDataFrameFromCsvWithHeader( "/policytransaction_writtendeductions/input/layer_trust_fund_PrimaryTestData.csv" )
    val line : DataFrame = utils.populateDataFrameFromCsvWithHeader( "/policytransaction_writtendeductions/input/line_PrimaryTestData.csv" )
    val line_risk_code : DataFrame = utils.populateDataFrameFromCsvWithHeader( "/policytransaction_writtendeductions/input/line_risk_code_PrimaryTestData.csv" )
    val lookup_deduction_type : DataFrame = utils.populateDataFrameFromCsvWithHeader( "/policytransaction_writtendeductions/input/lookup_deduction_type_PrimaryTestData.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/ndex/PolicyTransactionWrittenDeductions.hql" )
    val reconStatementInput = utils.loadHQLStatementFromResource( "Reconciliation/PolicyTransactionWrittenDeductions/InputRecordCount.hql" )
    val reconStatementOutput = utils.loadHQLStatementFromResource( "Reconciliation/PolicyTransactionWrittenDeductions/OutputRecordCount.hql" )

    // Act //
    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    layer_deduction.registerTempTable( "layer_deduction" )
    layer_trust_fund.registerTempTable( "layer_trust_fund" )
    line_risk_code.registerTempTable( "line_risk_code" )
    lookup_deduction_type.registerTempTable( "lookup_deduction_type" )
    sqlc.udf.register( "net_as_pct_of_gross", NetAsPctOfGross )

    val output = SQLRunner.runStatement( statement, sqlc )
    output.registerTempTable( "policytransaction_writtendeductions" )

    val reconInput = SQLRunner.runStatement( reconStatementInput, sqlc )
    val reconOutput = SQLRunner.runStatement( reconStatementOutput, sqlc )

    // Assert //
    assertDataFrameEquals( reconInput, reconOutput )
  }
}
