package com.kainos.enstar.test.TransformationUnitTesting.TrustFund

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, TransformationUnitTestingUtils, TrustFundUtils }
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.scalatest.FunSuite

/**
 * Created by caoimheb on 07/12/2016.
 */
class TransformationTests extends FunSuite with DataFrameSuiteBase {

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

  def populateDataFrameWithTrustFundTestData( dataFileName : String, sqlc : SQLContext ) : DataFrame = {

    utils.populateDataFrameFromFile(
      getClass.getResource( "/trustfund/output/" + dataFileName ).toString,
      getClass.getResource( "/trustfund/schemas/trustfund.avro" ).toString,
      _.split( "," ),
      TrustFundUtils.trustFundMapping,
      sqlc
    )
  }

  test( "TrustFund transformation mapping test" ) {

    // Arrange
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    val lookup_trust_fund = this.populateDataFrameWithLookupTrustFundTestData( "lookup_trust_fund_PrimaryTestData.csv", sqlc )
    val expectedTrustFund = this.populateDataFrameWithTrustFundTestData( "trustfund_PrimaryTestData.csv", sqlc )

    val hqlStatement = utils.loadHQLStatementFromResource( "Transformation/TrustFund.hql" )

    // Act
    lookup_trust_fund.registerTempTable( "lookup_trust_fund" )
    val result = SQLRunner.runStatement( hqlStatement, sqlc )

    // Assert
    assertDataFrameEquals( expectedTrustFund, result )

  }

}
