package com.kainos.enstar.test.TransformationUnitTesting.FILCode

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, FILCodeUtils, TransformationUnitTestingUtils }
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

class ReconciliationTests extends FunSuite with DataFrameSuiteBase {

  test( "FILCode reconciliation over test data" ) {

    // Arrange //
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    val lookup_fil_code : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/filcode/input/lookup_fil_code.csv" ).toString,
      getClass.getResource( "/filcode/schemas/lookup_fil_code.avro" ).toString,
      _.split( "," ),
      FILCodeUtils.lookupFilCodeMapping,
      sqlContext
    )

    val hqlStatement = utils.loadHQLStatementFromResource( "Transformation/ndex/FILCode.hql" )
    val reconStatementInput = utils.loadHQLStatementFromResource( "Reconciliation/FILCode/InputRecordCounts.hql" )
    val reconStatementOutput = utils.loadHQLStatementFromResource( "Reconciliation/FILCode/OutputRecordCounts.hql" )

    // Act //
    lookup_fil_code.registerTempTable( "lookup_fil_code" )

    val output = SQLRunner.runStatement( hqlStatement, sqlc )
    output.registerTempTable( "filcode" )

    val reconInput = SQLRunner.runStatement( reconStatementInput, sqlc )
    val reconOutput = SQLRunner.runStatement( reconStatementOutput, sqlc )

    // Assert //
    assertDataFrameEquals( reconInput, reconOutput )
  }

}
