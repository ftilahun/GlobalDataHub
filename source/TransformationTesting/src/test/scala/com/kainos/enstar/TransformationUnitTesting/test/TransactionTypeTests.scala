package com.kainos.enstar.TransformationUnitTesting.test

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{TransactionTypeUtils, SQLRunner, TransformationUnitTestingUtils}
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

class TransactionTypeTests extends FunSuite with DataFrameSuiteBase {

  test("TransactionType transformation mapping test") {

    // Arrange
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    val lookup_premium_type : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource("/transactiontype/input/lookup_premium_type.csv").toString,
      getClass.getResource("/transactiontype/schemas/lookup_premium_type.avro").toString,
      TransactionTypeUtils.lookupPremiumTypeMapping,
      sqlc
    )

    val expectedTransactionType : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource("/transactiontype/output/transaction_type.csv").toString,
      getClass.getResource("/transactiontype/schemas/transaction_type.avro").toString,
      TransactionTypeUtils.transactionTypeMapping,
      sqlc
    )

    val hqlStatement = utils.loadHQLStatementFromResource("Transformation/TransactionType.hql")

    // Act
    lookup_premium_type.registerTempTable("lookup_premium_type")
    val result = SQLRunner.runStatement(hqlStatement, sqlc)

    // Assert
    assertDataFrameEquals(expectedTransactionType, result)

  }
}
