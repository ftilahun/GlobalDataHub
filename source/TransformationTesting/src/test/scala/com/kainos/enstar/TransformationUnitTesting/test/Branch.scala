package com.kainos.enstar.TransformationUnitTesting.test

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ BranchUtils, SQLRunner, TransformationUnitTestingUtils }
import org.apache.spark.sql.types.{ StructField, StructType }
import org.scalatest.FunSuite
/**
 * Created by terences on 20/11/2016.
 */
class Branch extends FunSuite with DataFrameSuiteBase {

  test( "BranchTransformation_test1" ){

    // Arrange //
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext

    // Load test data into dataframe
    val lookup_profit_centre = TransformationUnitTestingUtils.populateDataFrame(
      getClass.getResource( "/branch/input/lookup_profit_centre_test1.csv" ).toString,
      getClass.getResource( "/branch/schemas/lookup_profit_centre.avro" ).toString,
      BranchUtils.lookupProfitCentreMapping,
      sqlContext
    )

    // Load expected result into dataframe
    val expectedBranch = TransformationUnitTestingUtils.populateDataFrame(
      getClass.getResource( "/branch/output/branch_test1.csv" ).toString,
      getClass.getResource( "/branch/schemas/branch.avro" ).toString,
      BranchUtils.branchMapping,
      sqlContext
    )

    // Load the hqp statement under test
    val statement = SQLRunner.loadStatementFromResource( "Branch.hql" )

    // Act //
    lookup_profit_centre.registerTempTable( "lookup_profit_centre" )
    val result = SQLRunner.runStatement( statement, sqlContext )

    // Make all types nullable, if not we will get errors from populated nullable fields
    val actual = sqlc.createDataFrame( result.rdd,
      StructType( result.schema.fields
        .map( b => StructField( b.name, b.dataType, true ) )
      )
    )
    val expected = sqlc.createDataFrame( expectedBranch.rdd,
      StructType( expectedBranch.schema.fields
        .map( b => StructField( b.name, b.dataType, true ) )
      )
    )

    // Assert //
    assertDataFrameEquals( expected, actual )
  }
}
