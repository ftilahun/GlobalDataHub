package com.kainos.enstar.test.TransformationUnitTesting.MethodOfPlacement

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, TransformationUnitTestingUtils }
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.scalatest.FunSuite

/**
 * Created by caoimheb on 08/12/2016.
 */
class ReconciliationTests extends FunSuite with DataFrameSuiteBase {

  private val utils = new TransformationUnitTestingUtils

  def populateDataFrameWithLookupBusinessTypeTestData( dataFileName : String )( implicit sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromCsvWithHeader( "/ndex/methodofplacement/input/" + dataFileName )
  }

  test( "MethodOfPlacement reconciliation over test data" ) {

    // Arrange //
    implicit val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    val lookup_trust_fund = this.populateDataFrameWithLookupBusinessTypeTestData( "PrimaryTestData.csv" )

    val hqlStatement = utils.loadHQLStatementFromResource( "Transformation/ndex/MethodOfPlacement.hql" )
    val reconStatementInput = utils.loadHQLStatementFromResource( "Reconciliation/MethodOfPlacement/InputRecordCount.hql" )
    val reconStatementOutput = utils.loadHQLStatementFromResource( "Reconciliation/MethodOfPlacement/OutputRecordCount.hql" )

    // Act //
    lookup_trust_fund.registerTempTable( "lookup_business_type" )

    val output = SQLRunner.runStatement( hqlStatement, sqlc )
    output.registerTempTable( "ndex/methodofplacement" )

    val reconInput = SQLRunner.runStatement( reconStatementInput, sqlc )
    val reconOutput = SQLRunner.runStatement( reconStatementOutput, sqlc )

    // Assert //
    assertDataFrameEquals( reconInput, reconOutput )
  }

}
