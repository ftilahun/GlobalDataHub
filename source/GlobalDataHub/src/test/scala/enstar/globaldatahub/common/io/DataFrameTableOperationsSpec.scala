package enstar.globaldatahub.common.io

import enstar.globaldatahub.TestContexts
import org.scalatest.{ FlatSpec, GivenWhenThen, Matchers }

/**
 * unit tests for CDCTableOperations
 */
class DataFrameTableOperationsSpec
    extends FlatSpec
    with GivenWhenThen
    with Matchers {

  "DataFrameTableOperations" should "Register and drop a table" in {
    val tableName = "DummyData"
    val cdcTableOperations = new DataFrameTableOperations()
    Given("A query on a table ")
    When("The table has 10 rows")
    cdcTableOperations.registerTempTable(TestContexts.dummyData(10), tableName)
    Then("10 rows should be returned from a SQL Query")
    TestContexts.sqlContext.sql(s"select * from $tableName").count should be(
      10)

    Given("A query on a table")
    When("The table does not exist")
    Then("An exception should be raised")
    cdcTableOperations.deRegisterTempTable(TestContexts.sqlContext, tableName)
    an[RuntimeException] should be thrownBy {
      TestContexts.sqlContext.sql(s"select * from $tableName").count should be(
        10)
    }
  }
}
