package com.kainos.enstar.test.TransformationUnitTesting.Currency

/**
 * Created by adamf on 29/11/2016.
 */

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, TransformationUnitTestingUtils }
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

class ReconciliationTests extends FunSuite with DataFrameSuiteBase {

  test( "Currency Mapping reconciliation over test data" ) {

    // Arrange //
    implicit val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    val lookup_currency = utils.populateDataFrameFromCsvWithHeader( "/ndex/currency/input/lookup_currency/PrimaryTestData.csv" )
    lookup_currency.registerTempTable( "lookup_currency" )

    val currency = utils.populateDataFrameFromCsvWithHeader( "/ndex/currency/output/PrimaryTestData.csv" )
    currency.registerTempTable( "currency" )

    val hqlStatement = utils.loadHQLStatementFromResource( "Transformation/ndex/Currency.hql" )
    val reconStatementInput = utils.loadHQLStatementFromResource( "Reconciliation/Currency/InputRecordCount.hql" )
    val reconStatementOutput = utils.loadHQLStatementFromResource( "Reconciliation/Currency/OutputRecordCount.hql" )

    // Act //

    val output = SQLRunner.runStatement( hqlStatement, sqlc )
    output.registerTempTable( "currency" )

    val reconInput = SQLRunner.runStatement( reconStatementInput, sqlc )
    val reconOutput = SQLRunner.runStatement( reconStatementOutput, sqlc )

    // Assert //
    assertDataFrameEquals( reconInput, reconOutput )
  }

}
