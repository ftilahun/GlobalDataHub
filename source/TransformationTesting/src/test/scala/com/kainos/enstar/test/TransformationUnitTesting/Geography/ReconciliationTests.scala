package com.kainos.enstar.test.TransformationUnitTesting.Geography

/**
 * Created by adamf on 29/11/2016.
 */

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ GeographyUtils, SQLRunner, TransformationUnitTestingUtils }
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

class ReconciliationTests extends FunSuite with DataFrameSuiteBase {

  test( "Geography Mapping reconciliation over test data" ) {

    // Arrange //
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    val lookup_country : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/geography/input/lookup_country_standard_data.csv" ).toString,
      getClass.getResource( "/geography/schemas/lookup_country.avro" ).toString,
      _.split( "," ),
      GeographyUtils.lookupCountryMapping,
      sqlContext
    )

    val hqlStatement = utils.loadHQLStatementFromResource( "Transformation/Geography.hql" )
    val reconStatementInput = utils.loadHQLStatementFromResource( "Reconciliation/Geography/InputRecordCount.hql" )
    val reconStatementOutput = utils.loadHQLStatementFromResource( "Reconciliation/Geography/OutputRecordCount.hql" )

    // Act //
    lookup_country.registerTempTable( "lookup_country" )

    val output = SQLRunner.runStatement( hqlStatement, sqlc )
    output.registerTempTable( "geography" )

    val reconInput = SQLRunner.runStatement( reconStatementInput, sqlc )
    val reconOutput = SQLRunner.runStatement( reconStatementOutput, sqlc )

    // Assert //
    assertDataFrameEquals( reconInput, reconOutput )
  }

}
