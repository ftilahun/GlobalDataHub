package com.kainos.enstar.test.TransformationUnitTesting.TransactionType

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{
  SQLRunner,
  TransformationUnitTestingUtils
}
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

class ReconciliationTests extends FunSuite with DataFrameSuiteBase {

  test( "TransactionType-WrittenPremium reconciliation over test data" ) {

    // Arrange //
    val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )
    val utils = new TransformationUnitTestingUtils

    val hqlStatement = utils.loadHQLStatementFromResource(
      "Transformation/ndex/TransactionTypeWrittenPremium.hql" )
    val reconStatementOutput = utils.loadHQLStatementFromResource(
      "Reconciliation/TransactionTypeWrittenPremium/OutputRecordCounts.hql" )

    // Act //
    val output = SQLRunner.runStatement( hqlStatement, sqlc )
    output.registerTempTable( "transactiontype" )

    val reconOutput = SQLRunner.runStatement( reconStatementOutput, sqlc )

    // Assert //
    assert( reconOutput.count == 1 )
  }

  test( "TransactionType-Written Deductions reconciliation over test data" ) {

    // Arrange //
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )
    val utils = new TransformationUnitTestingUtils

    val lookup_premium_type : DataFrame = utils.populateDataFrameFromCsvWithHeader( "/ndex/transactiontype/writtendeductions/input/lookup_deduction_type/PrimaryTestData.csv" )

    val hqlStatement = utils.loadHQLStatementFromResource(
      "Transformation/ndex/TransactionTypeWrittenDeduction.hql" )
    val reconStatementInput = utils.loadHQLStatementFromResource(
      "Reconciliation/TransactionTypeWrittenDeductions/InputRecordCounts.hql" )
    val reconStatementOutput = utils.loadHQLStatementFromResource(
      "Reconciliation/TransactionTypeWrittenDeductions/OutputRecordCounts.hql" )

    // Act //
    lookup_premium_type.registerTempTable( "lookup_deduction_type" )

    val output = SQLRunner.runStatement( hqlStatement, sqlc )
    output.registerTempTable( "transactiontype" )

    val reconInput = SQLRunner.runStatement( reconStatementInput, sqlc )
    val reconOutput = SQLRunner.runStatement( reconStatementOutput, sqlc )

    // Assert //
    assertDataFrameEquals( reconInput, reconOutput )
  }
}
