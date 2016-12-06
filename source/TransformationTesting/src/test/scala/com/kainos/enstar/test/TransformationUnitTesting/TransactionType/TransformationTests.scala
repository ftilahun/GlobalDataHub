package com.kainos.enstar.test.TransformationUnitTesting.TransactionType

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, TransactionTypeUtils, TransformationUnitTestingUtils }
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

class TransformationTests extends FunSuite with DataFrameSuiteBase {

  test( "TransactionType transformation mapping test" ) {

    // Arrange
    val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )
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

  test( "TransactionType - Written Deductions mapping with Primary Test Data" ){

    // Arrange
    val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )
    val utils = new TransformationUnitTestingUtils

    val lookup_deduction_type : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/transactiontype_writtendeductions/input/lookup_deduction_type_PrimaryTestData.csv" ).toString,
      getClass.getResource( "/transactiontype_writtendeductions/schemas/lookup_deduction_type.avro" ).toString,
      _.split( "," ),
      TransactionTypeUtils.lookupPremiumTypeMapping,
      sqlc
    )

    val expectedTransactionType : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/transactiontype_writtendeductions/output/transactiontype_PrimaryTestData.csv" ).toString,
      getClass.getResource( "/transactiontype_writtendeductions/schemas/transactiontype.avro" ).toString,
      _.split( "," ),
      TransactionTypeUtils.transactionTypeMapping,
      sqlc
    )

    val hqlStatement = utils.loadHQLStatementFromResource( "Transformation/TransactionTypeWrittenDeduction.hql" )

    // Act
    lookup_deduction_type.registerTempTable( "lookup_deduction_type" )
    val result = SQLRunner.runStatement( hqlStatement, sqlc )

    // Assert
    assertDataFrameEquals( expectedTransactionType, result )

    val expectedRowCount = 5
    assert( expectedTransactionType.count() == expectedRowCount )

  }
}
