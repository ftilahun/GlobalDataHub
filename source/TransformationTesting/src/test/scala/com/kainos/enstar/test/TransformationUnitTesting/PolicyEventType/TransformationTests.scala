package com.kainos.enstar.test.TransformationUnitTesting.Branch

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, TransformationUnitTestingUtils }
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

/**
 * Created by terences on 20/11/2016.
 */
class TransformationTests extends FunSuite with DataFrameSuiteBase {

  test( "BranchTransformation tes with Priamry data" ){

    // Arrange //
    // Use sqlContext from spark-testing-base
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )
    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframe
    val lookupProfitCentre = utils.populateDataFrameFromCsvWithHeader( "/branch/input/lookup_profit_centre_PrimaryTestData.csv" )

    // Load expected result into dataframe
    val expectedBranch = utils.populateDataFrameFromCsvWithHeader( "/branch/output/branch_PriamryTestData.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/Branch.hql" )

    // Act //
    lookupProfitCentre.registerTempTable( "lookup_profit_centre" )
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedBranch, result )
  }
}
