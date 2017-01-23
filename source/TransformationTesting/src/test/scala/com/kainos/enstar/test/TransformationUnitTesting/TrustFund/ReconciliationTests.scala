package com.kainos.enstar.test.TransformationUnitTesting.TrustFund

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, TransformationUnitTestingUtils }
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.scalatest.FunSuite

/**
 * Created by caoimheb on 07/12/2016.
 */
class ReconciliationTests extends FunSuite with DataFrameSuiteBase {

  private val utils = new TransformationUnitTestingUtils

  def populateDataFrameWithLookupTrustFundTestData( dataFileName : String )( implicit sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromCsvWithHeader( "/ndex/trustfund/input/" + dataFileName )
  }

  test( "TrustFund reconciliation over test data" ) {

    // Arrange //
    implicit val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    val lookup_trust_fund = this.populateDataFrameWithLookupTrustFundTestData( "PrimaryTestData.csv" )

    val hqlStatement = utils.loadHQLStatementFromResource( "Transformation/ndex/TrustFund.hql" )
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
