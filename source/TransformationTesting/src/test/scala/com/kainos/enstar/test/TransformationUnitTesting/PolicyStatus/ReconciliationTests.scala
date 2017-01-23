package com.kainos.enstar.test.TransformationUnitTesting.PolicyStatus

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, TransformationUnitTestingUtils }
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.scalatest.FunSuite

/**
 * Created by caoimheb on 07/12/2016.
 */
class ReconciliationTests extends FunSuite with DataFrameSuiteBase {

  private val utils = new TransformationUnitTestingUtils

  def populateDataFrameWithLookupLineStatusTestData( dataFileName : String )( implicit sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromCsvWithHeader( "/ndex/policystatus/input/" + dataFileName )
  }

  test( "PolicyStatus reconciliation over test data" ) {

    // Arrange //
    implicit val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    val lookup_line_status = this.populateDataFrameWithLookupLineStatusTestData( "PrimaryTestData.csv" )

    val hqlStatement = utils.loadHQLStatementFromResource( "Transformation/ndex/PolicyStatus.hql" )
    val reconStatementInput = utils.loadHQLStatementFromResource( "Reconciliation/PolicyStatus/InputRecordCount.hql" )
    val reconStatementOutput = utils.loadHQLStatementFromResource( "Reconciliation/PolicyStatus/OutputRecordCount.hql" )

    // Act //
    lookup_line_status.registerTempTable( "lookup_line_status" )

    val output = SQLRunner.runStatement( hqlStatement, sqlc )
    output.registerTempTable( "policystatus" )

    val reconInput = SQLRunner.runStatement( reconStatementInput, sqlc )
    val reconOutput = SQLRunner.runStatement( reconStatementOutput, sqlc )

    // Assert //
    assertDataFrameEquals( reconInput, reconOutput )
  }

}
