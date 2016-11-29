package com.kainos.enstar.test.TransformationUnitTesting.TransactionType

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ TransactionTypeUtils, SQLRunner, TransformationUnitTestingUtils }
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

class ReconciliationTests extends FunSuite with DataFrameSuiteBase {

  test( "TransactionType reconciliation over test data" ) {

    // Arrange //
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    val lookup_premium_type : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/transactiontype/input/lookup_premium_type.csv" ).toString,
      getClass.getResource( "/transactiontype/schemas/lookup_premium_type.avro" ).toString,
      _.split( "," ),
      TransactionTypeUtils.lookupPremiumTypeMapping,
      sqlContext
    )

    val hqlStatement = utils.loadHQLStatementFromResource( "Transformation/TransactionType.hql" )
    val reconStatementInput = utils.loadHQLStatementFromResource( "Reconciliation/TransactionType/InputRecordCounts.hql" )
    val reconStatementOutput = utils.loadHQLStatementFromResource( "Reconciliation/TransactionType/OutputRecordCounts.hql" )

    // Act //
    lookup_premium_type.registerTempTable( "lookup_premium_type" )

    val output = SQLRunner.runStatement( hqlStatement, sqlc )
    output.registerTempTable( "transactiontype" )

    val reconInput = SQLRunner.runStatement( reconStatementInput, sqlc )
    val reconOutput = SQLRunner.runStatement( reconStatementOutput, sqlc )

    // Assert //
    assertDataFrameEquals( reconInput, reconOutput )
  }

}
