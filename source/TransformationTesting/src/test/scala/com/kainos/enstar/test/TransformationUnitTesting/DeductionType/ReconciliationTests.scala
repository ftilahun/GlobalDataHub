package com.kainos.enstar.test.TransformationUnitTesting.DeductionType

/**
 * Created by adamf on 30/11/2016.
 */

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ AnalysisCodeSplitRiskCodeUtils, DeductionTypeUtils, SQLRunner, TransformationUnitTestingUtils }
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

class ReconciliationTests extends FunSuite with DataFrameSuiteBase {
  test( "DeductionType Mapping reconciliation over test data" ) {

    // Arrange //
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    val lookup_deduction_type : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/deductiontype/input/lookup_deduction_type_standard_data.csv" ).toString,
      getClass.getResource( "/deductiontype/schema/lookup_deduction_type.avro" ).toString,
      _.split( "," ),
      DeductionTypeUtils.lookupDeductionTypeMapping,
      sqlc
    )

    val hqlStatement = utils.loadHQLStatementFromResource( "Transformation/DeductionType.hql" )
    val reconStatementInput = utils.loadHQLStatementFromResource( "Reconciliation/DeductionType/InputRecordCount.hql" )
    val reconStatementOutput = utils.loadHQLStatementFromResource( "Reconciliation/DeductionType/OutputRecordCount.hql" )

    // Act //
    lookup_deduction_type.registerTempTable( "lookup_deduction_type" )

    val output = SQLRunner.runStatement( hqlStatement, sqlc )
    output.registerTempTable( "deductiontype" )

    val reconInput = SQLRunner.runStatement( reconStatementInput, sqlc )
    val reconOutput = SQLRunner.runStatement( reconStatementOutput, sqlc )

    // Assert //
    assertDataFrameEquals( reconInput, reconOutput )
  }
}
