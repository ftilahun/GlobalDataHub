package com.kainos.enstar.test.TransformationUnitTesting.Branch

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ BranchUtils, SQLRunner, TransformationUnitTestingUtils }
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite
/**
 * Created by terences on 20/11/2016.
 */
class TransformationTests extends FunSuite with DataFrameSuiteBase {

  test( "BranchTransformation_test1" ){

    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Arrange //
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframe
    val lookup_profit_centre : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/branch/input/lookup_profit_centre_test1.csv" ).toString,
      getClass.getResource( "/branch/schemas/lookup_profit_centre.avro" ).toString,
      BranchUtils.lookupProfitCentreMapping,
      sqlc
    )

    // Load expected result into dataframe
    val expectedBranch : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/branch/output/branch_test1.csv" ).toString,
      getClass.getResource( "/branch/schemas/branch.avro" ).toString,
      BranchUtils.branchMapping,
      sqlc
    )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/Branch.hql" )

    // Act //
    lookup_profit_centre.registerTempTable( "lookup_profit_centre" )
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedBranch, result )
  }
}
