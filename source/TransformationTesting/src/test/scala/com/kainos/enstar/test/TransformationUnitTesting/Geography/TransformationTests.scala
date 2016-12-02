package com.kainos.enstar.test.TransformationUnitTesting.Geography

/**
 * Created by adamf on 29/11/2016.
 */
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ GeographyUtils, SQLRunner, TransformationUnitTestingUtils }
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

class TransformationTests extends FunSuite with DataFrameSuiteBase {

  test( "Geography Standard Data test1" ){
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    val lookup_country : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/geography/input/lookup_country_standard_data.csv" ).toString,
      getClass.getResource( "/geography/schemas/lookup_country.avro" ).toString,
      _.split( "," ),
      GeographyUtils.lookupCountryMapping,
      sqlContext
    )

    val expectedGeographyMapping : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/geography/output/geography_standard_data.csv" ).toString,
      getClass.getResource( "/geography/schemas/geography.avro" ).toString,
      _.split( "," ),
      GeographyUtils.geographyMapping,
      sqlContext
    )

    val statement = utils.loadHQLStatementFromResource( "Transformation/Geography.hql" )

    lookup_country.registerTempTable( "lookup_country" )
    val result = SQLRunner.runStatement( statement, sqlc )

    assertDataFrameEquals( expectedGeographyMapping, result )
  }
}

