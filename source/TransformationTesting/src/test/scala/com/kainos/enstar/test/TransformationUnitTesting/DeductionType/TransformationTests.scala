package com.kainos.enstar.test.TransformationUnitTesting.DeductionType

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ BranchUtils, DeductionTypeUtils, SQLRunner, TransformationUnitTestingUtils }
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

/**
 * Created by adamf on 30/11/2016.
 */
class TransformationTests extends FunSuite with DataFrameSuiteBase {
  test( "DeductionMappingTransformation_test1" ) {
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    val lookup_deduction_type : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/deductiontype/input/lookup_deduction_type_standard_data.csv" ).toString,
      getClass.getResource( "/deductiontype/schema/lookup_deduction_type.avro" ).toString,
      _.split( "," ),
      DeductionTypeUtils.lookupDeductionTypeMapping,
      sqlc
    )

    // Load expected results into dataframe
    val expectedDeductionTypeMapping : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/deductiontype/output/deductiontype_standard_data.csv" ).toString,
      getClass.getResource( "/deductiontype/schema/deductiontype.avro" ).toString,
      _.split( "," ),
      BranchUtils.branchMapping,
      sqlc
    )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/DeductionType.hql" )

    // Act //
    lookup_deduction_type.registerTempTable( "lookup_deduction_type" )
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedDeductionTypeMapping, result )
  }
}
