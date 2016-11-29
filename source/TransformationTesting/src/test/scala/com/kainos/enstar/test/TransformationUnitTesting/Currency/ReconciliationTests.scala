package com.kainos.enstar.test.TransformationUnitTesting.Currency

/**
 * Created by adamf on 29/11/2016.
 */

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{CurrencyUtils, SQLRunner, TransformationUnitTestingUtils}
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

class ReconciliationTests extends FunSuite with DataFrameSuiteBase {

  test( "Currency Mapping reconciliation over test data" ) {

    // Arrange //
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    val lookup_currency : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource("/currency/schemas/lookup_currency.avsc").toString,
      getClass.getResource("/currency/schemas/lookup_currency.avro").toString,
      _.split( "," ),
      CurrencyUtils.lookupCurrencyMapping,
      sqlContext
    )

    val hqlStatement = utils.loadHQLStatementFromResource("Transformation/Currency.hql")
    val reconStatementInput = utils.loadHQLStatementFromResource("Reconciliation/Currency/InputRecordCount.hql")
    val reconStatementOutput = utils.loadHQLStatementFromResource("Reconciliation/Currency/OutputRecordCount.hql")

    // Act //
    lookup_currency.registerTempTable( "lookup_currency" )

    val output = SQLRunner.runStatement( hqlStatement, sqlc )
    output.registerTempTable( "currency" )

    val reconInput = SQLRunner.runStatement( reconStatementInput, sqlc )
    val reconOutput = SQLRunner.runStatement( reconStatementOutput, sqlc )

    // Assert //
    assertDataFrameEquals( reconInput, reconOutput )
  }

}
