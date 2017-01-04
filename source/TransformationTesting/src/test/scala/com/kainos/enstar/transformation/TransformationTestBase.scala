package com.kainos.enstar.transformation

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.TransformationUnitTestingUtils
import org.scalatest.FunSuite

/** Base class to test a query against a set of source tables and compare output to expected result.
  *
  * The source tables are specified using the sourceTableLocations parameter. This class loads
  * data from the CSV files specified in the values of this map into DataFrames. The CSV files
  * must have a header row giving column names and types. The resulting DataFrame is registered
  * as a temporary table against the sqlContext using the key as the name.
  *
  * @param testName the name of the test to be included in the test output
  * @param sourceTableLocations a map of table names (used by the query) to CSV file locations
  * @param expectedTableLocation location of expected result CSV file (with headers)
  * @param statementLocation location of the query to test
  */
abstract class TransformationTestBase( testName : String,
                                       sourceTableLocations : Map[String, String],
                                       expectedTableLocation : String,
                                       statementLocation : String ) extends FunSuite with DataFrameSuiteBase {

  // must be def rather than val because sqlContext is null during instantiation
  implicit def sqlc = sqlContext

  val utils = new TransformationUnitTestingUtils

  override def beforeAll() {
    // Arrange
    super.beforeAll()

    for ( ( tableName, tableSourceLocation ) <- sourceTableLocations.toList ) {
      utils.populateDataFrameFromCsvWithHeader( tableSourceLocation ).registerTempTable( tableName )
    }
  }

  test( testName ) {
    // Act
    val expected = utils.populateDataFrameFromCsvWithHeader( expectedTableLocation )
    val statement = utils.loadHQLStatementFromResource( statementLocation )
    val result = sqlc.sql( statement )

    // Assert
    assertDataFrameEquals( expected, result )
  }
}
