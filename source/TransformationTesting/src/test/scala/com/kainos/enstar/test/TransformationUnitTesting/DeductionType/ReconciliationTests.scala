package com.kainos.enstar.test.TransformationUnitTesting.DeductionType

/**
 * Created by adamf on 30/11/2016.
 */

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, TransformationUnitTestingUtils }
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

class ReconciliationTests extends FunSuite with DataFrameSuiteBase {
  test( "DeductionType Mapping reconciliation over test data" ) {

    // Arrange //
    implicit val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    val lookup_deduction_type = utils.populateDataFrameFromCsvWithHeader( "/ndex/deductiontype/input/lookup_deduction_type/PrimaryTestData.csv" )
    lookup_deduction_type.registerTempTable( "lookup_deduction_type" )

    val hqlStatement = utils.loadHQLStatementFromResource( "Transformation/ndex/DeductionType.hql" )
    val reconStatementInput = utils.loadHQLStatementFromResource( "Reconciliation/DeductionType/InputRecordCount.hql" )
    val reconStatementOutput = utils.loadHQLStatementFromResource( "Reconciliation/DeductionType/OutputRecordCount.hql" )

    // Act //

    val output = SQLRunner.runStatement( hqlStatement, sqlc )
    output.registerTempTable( "ndex/deductiontype" )

    val reconInput = SQLRunner.runStatement( reconStatementInput, sqlc )
    val reconOutput = SQLRunner.runStatement( reconStatementOutput, sqlc )

    // Assert //
    assertDataFrameEquals( reconInput, reconOutput )
  }
}
