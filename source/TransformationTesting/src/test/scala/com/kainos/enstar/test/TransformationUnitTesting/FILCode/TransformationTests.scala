package com.kainos.enstar.test.TransformationUnitTesting.FILCode

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, FILCodeUtils, TransformationUnitTestingUtils }
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

class TransformationTests extends FunSuite with DataFrameSuiteBase {

  test( "TransactionType transformation mapping test" ) {

    // Arrange
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    val lookup_fil_code : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/filcode/input/lookup_fil_code.csv" ).toString,
      getClass.getResource( "/filcode/schemas/lookup_fil_code.avro" ).toString,
      _.split( "," ),
      FILCodeUtils.lookupFilCodeMapping,
      sqlc
    )

    val expectedFilCode : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/filcode/output/filcode.csv" ).toString,
      getClass.getResource( "/filcode/schemas/filcode.avro" ).toString,
      _.split( "," ),
      FILCodeUtils.filCodeMapping,
      sqlc
    )

    val hqlStatement = utils.loadHQLStatementFromResource( "Transformation/FILCode.hql" )

    // Act
    lookup_fil_code.registerTempTable( "lookup_fil_code" )
    val result = SQLRunner.runStatement( hqlStatement, sqlc )

    // Assert
    assertDataFrameEquals( expectedFilCode, result )

    val expectedRowCount = 4
    assert( expectedFilCode.count() == expectedRowCount )

  }
}
