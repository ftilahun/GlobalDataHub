package com.kainos.enstar.test.TransformationUnitTesting.Currency

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{CurrencyUtils, SQLRunner, TransformationUnitTestingUtils}
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

/**
 * Created by adamf on 23/11/2016.
 */
class TransformationTests extends FunSuite with DataFrameSuiteBase {
  test( "CurrencyMappingTransformation_test1" ) {
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    val lookup_currency : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource("/currency/input/lookup_currency_test1.csv").toString,
      getClass.getResource("/currency/schemas/lookup_currency.avro").toString,
      _.split( "," ),
      CurrencyUtils.lookupCurrencyMapping,
      sqlc
    )

    // Load Expected Results into dataframe
    val expectedCurrencyMapping : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource("/currency/output/currency_mapping_test1.csv").toString,
      getClass.getResource("/currency/schemas/currency_mapping.avro").toString,
      _.split( "," ),
      CurrencyUtils.currencyMapping,
      sqlc
    )

    val statement = utils.loadHQLStatementFromResource("Transformation/Currency.hql")

    lookup_currency.registerTempTable( "lookup_currency" )
    val result = SQLRunner.runStatement( statement, sqlc )

    assertDataFrameEquals( expectedCurrencyMapping, result )

  }
}
