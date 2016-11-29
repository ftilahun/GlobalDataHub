package com.kainos.enstar.test.TransformationUnitTesting.TransactionType

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, TransactionTypeUtils, TransformationUnitTestingUtils }
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

class TransformationTests extends FunSuite with DataFrameSuiteBase {

  test( "TransactionType transformation mapping test" ) {

    // Arrange
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    val lookup_premium_type : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/transactiontype/input/lookup_premium_type.csv" ).toString,
      getClass.getResource( "/transactiontype/schemas/lookup_premium_type.avro" ).toString,
      _.split( "," ),
      TransactionTypeUtils.lookupPremiumTypeMapping,
      sqlc
    )

    val expectedTransactionType : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/transactiontype/output/transactiontype.csv" ).toString,
      getClass.getResource( "/transactiontype/schemas/transactiontype.avro" ).toString,
      _.split( "," ),
      TransactionTypeUtils.transactionTypeMapping,
      sqlc
    )

    val hqlStatement = utils.loadHQLStatementFromResource( "Transformation/TransactionType.hql" )

    // Act
    lookup_premium_type.registerTempTable( "lookup_premium_type" )
    val result = SQLRunner.runStatement( hqlStatement, sqlc )

    // Assert
    assertDataFrameEquals( expectedTransactionType, result )

    val expectedRowCount = 5
    assert( expectedTransactionType.count() == expectedRowCount )

  }
}
