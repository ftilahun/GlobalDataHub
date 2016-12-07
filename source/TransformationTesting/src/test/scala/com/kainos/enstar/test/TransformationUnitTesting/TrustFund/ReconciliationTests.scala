package com.kainos.enstar.test.TransformationUnitTesting.TrustFund

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, TransformationUnitTestingUtils, TrustFundUtils }
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.scalatest.FunSuite

/**
 * Created by caoimheb on 07/12/2016.
 */
class ReconciliationTests extends FunSuite with DataFrameSuiteBase {

  private val utils = new TransformationUnitTestingUtils

  def populateDataFrameWithLookupTrustFundTestData( dataFileName : String, sqlc : SQLContext ) : DataFrame = {

    utils.populateDataFrameFromFile(
      getClass.getResource( "/trustfund/input/" + dataFileName ).toString,
      getClass.getResource( "/trustfund/schemas/lookup_trust_fund.avro" ).toString,
      _.split( "," ),
      TrustFundUtils.lookupTrustFundMapping,
      sqlc
    )
  }

  test( "TrustFund reconciliation over test data" ) {

    // Arrange //
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    val lookup_trust_fund = this.populateDataFrameWithLookupTrustFundTestData( "lookup_trust_fund_PrimaryTestData.csv", sqlc )

    val hqlStatement = utils.loadHQLStatementFromResource( "Transformation/TrustFund.hql" )
    val reconStatementInput = utils.loadHQLStatementFromResource( "Reconciliation/TrustFund/InputRecordCounts.hql" )
    val reconStatementOutput = utils.loadHQLStatementFromResource( "Reconciliation/TrustFund/OutputRecordCounts.hql" )

    // Act //
    lookup_trust_fund.registerTempTable( "lookup_trust_fund" )

    val output = SQLRunner.runStatement( hqlStatement, sqlc )
    output.registerTempTable( "trustfund" )

    val reconInput = SQLRunner.runStatement( reconStatementInput, sqlc )
    val reconOutput = SQLRunner.runStatement( reconStatementOutput, sqlc )

    // Assert //
    assertDataFrameEquals( reconInput, reconOutput )
  }
}
