package com.kainos.enstar.test.TransformationUnitTesting.FILCode

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, TransformationUnitTestingUtils }
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

class ReconciliationTests extends FunSuite with DataFrameSuiteBase {

  test( "FILCode reconciliation over test data" ) {

    // Arrange //
    implicit val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    val lookup_fil_code = utils.populateDataFrameFromCsvWithHeader( "/ndex/filcode/input/lookup_fil_code/PrimaryTestData.csv" )

    val hqlStatement = utils.loadHQLStatementFromResource( "Transformation/ndex/FILCode.hql" )
    val reconStatementInput = utils.loadHQLStatementFromResource( "Reconciliation/FILCode/InputRecordCounts.hql" )
    val reconStatementOutput = utils.loadHQLStatementFromResource( "Reconciliation/FILCode/OutputRecordCounts.hql" )

    // Act //
    lookup_fil_code.registerTempTable( "lookup_fil_code" )

    val output = SQLRunner.runStatement( hqlStatement, sqlc )
    output.registerTempTable( "ndex/filcode" )

    val reconInput = SQLRunner.runStatement( reconStatementInput, sqlc )
    val reconOutput = SQLRunner.runStatement( reconStatementOutput, sqlc )

    // Assert //
    assertDataFrameEquals( reconInput, reconOutput )
  }

}
