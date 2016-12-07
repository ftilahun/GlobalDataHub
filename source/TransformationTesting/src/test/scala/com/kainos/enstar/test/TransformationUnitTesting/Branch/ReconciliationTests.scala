package com.kainos.enstar.test.TransformationUnitTesting.Branch

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ BranchUtils, SQLRunner, TransformationUnitTestingUtils }
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

/**
 * Created by adamf on 30/11/2016.
 */
class ReconciliationTests extends FunSuite with DataFrameSuiteBase {

  test( "Reconciliation over test data for Branch" ) {

    // Arrange //
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframe
    val lookup_profit_centre : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/branch/input/lookup_profit_centre_test1.csv" ).toString,
      getClass.getResource( "/branch/schemas/lookup_profit_centre.avro" ).toString,
      _.split( "," ),
      BranchUtils.lookupProfitCentreMapping,
      sqlContext
    )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/Branch.hql" )
    val reconStatementInput = utils.loadHQLStatementFromResource( "Reconciliation/Branch/InputRecordCount.hql" )
    val reconStatementOutput = utils.loadHQLStatementFromResource( "Reconciliation/Branch/OutputRecordCount.hql" )

    // Act //
    lookup_profit_centre.registerTempTable( "lookup_profit_centre" )
    val output = SQLRunner.runStatement( statement, sqlc )
    output.registerTempTable( "branch" )

    val reconInput = SQLRunner.runStatement( reconStatementInput, sqlc )
    val reconOutput = SQLRunner.runStatement( reconStatementOutput, sqlc )

    // Assert //
    assertDataFrameEquals( reconInput, reconOutput )
  }
}