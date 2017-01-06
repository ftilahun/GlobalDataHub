package com.kainos.enstar.test.TransformationUnitTesting.TransactionType

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, TransactionTypeUtils, TransformationUnitTestingUtils }
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

class TransformationTests extends FunSuite with DataFrameSuiteBase {

  val utils = new TransformationUnitTestingUtils

  test( "TransactionType-WrittenPremium transformation mapping test" ) {

    // Arrange
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    val expectedTransactionType : DataFrame = utils.populateDataFrameFromCsvWithHeader(
      "/transactiontype/output/transactiontype.csv"
    )

    val hqlStatement = utils.loadHQLStatementFromResource( "Transformation/TransactionTypeWrittenPremium.hql" )

    // Act
    val result = SQLRunner.runStatement( hqlStatement, sqlc )

    // Assert
    assertDataFrameEquals( expectedTransactionType, result )

  }

  test( "TransactionType - Written Deductions mapping with Primary Test Data" ){

    // Arrange
    val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    val lookup_deduction_type : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/transactiontype_writtendeductions/input/lookup_deduction_type_PrimaryTestData.csv" ).toString,
      getClass.getResource( "/transactiontype_writtendeductions/schemas/lookup_deduction_type.avro" ).toString,
      _.split( "," ),
      TransactionTypeUtils.lookupDeductionTypeMapping,
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

  }
}
