package enstar.cdctableprocessor.tests.integration

import enstar.cdctableprocessor.TestContexts
import org.scalatest.{ FlatSpec, GivenWhenThen, Matchers }
import enstar.cdctableprocessor.io.{
  AvroDataFrameReader,
  AvroDataFrameWriter,
  CDCDataFrameReader,
  CDCDataFrameWriter
}
import enstar.cdctableprocessor.processor.CDCTableProcessor
import enstar.cdctableprocessor.properties.CDCProperties
import enstar.cdctableprocessor.udfs.CDCUserFunctions
import org.apache.spark.storage.StorageLevel
import org.joda.time.format.DateTimeFormat

import scala.util.Try
import scalax.file.Path

/**
 * Integration test for CDCTableProcessor
 */
class CDCTableProcessorSpec extends FlatSpec with GivenWhenThen with Matchers {

  "CDCTableProcessor" should "Process a table" in {

    val attunityCutoff = "2017-02-01 01:00:00.000"

    /**
     * Directories used for IO
     */
    val changeInput = "./src/test/resources/cdctestdata/changes"
    val activeInput = "./src/test/resources/cdctestdata/active"
    val historyOutput = "./src/test/resources/cdctestdata/output/history"
    val activeOutput = "./src/test/resources/cdctestdata/output/active"
    val immatureChangesOutput =
      "./src/test/resources/cdctestdata/output/unprocessed"
    val metricsOutput = "./src/test/resources/cdctestdata/output/matrics"

    //delete any dirs from last run.
    deleteDir(historyOutput)
    deleteDir(activeOutput)
    deleteDir(immatureChangesOutput)
    deleteDir(metricsOutput)

    implicit val sQLContext = TestContexts.sqlContext

    val userFunctions = new CDCUserFunctions
    val writer = new CDCDataFrameWriter(new AvroDataFrameWriter)
    val reader = new CDCDataFrameReader(new AvroDataFrameReader)
    val tblProcessor = new CDCTableProcessor()
    val properties: CDCProperties = CDCProperties(
      idColumnName = "table__id",
      transactionTimeStampColumnName = "header__timestamp",
      operationColumnName = "header__change_oper",
      transactionIdColumnName = "header__transaction_id",
      changeSequenceColumnName = "header__change_seq",
      attunityColumnPrefix = "header__",
      validFromColumnName = "validfrom",
      validToColumnName = "validto",
      activeColumnName = "active",
      attunityCutoff = attunityCutoff,
      attunityDateFormat = "YYYY-MM-dd HH:mm:ss.SSS",
      attunityDateFormatShort = "YYYY-MM-dd HH:mm:ss",
      changeInputDir = changeInput,
      activeInput = activeInput,
      activeOutput = activeOutput,
      immatureChangesOutput = immatureChangesOutput,
      historyOutput = historyOutput,
      metricsOutputDir = Some(metricsOutput)
    )

    /**
     * READ CURRENT ACTIVE RECORDS
     */
    Given("an existing active partition with 3 records")
    val baseRecords = reader.read(activeInput, Some(StorageLevel.MEMORY_ONLY))

    /**
     * VALIDATE BASE RECORDS
     */
    baseRecords.count should be(3)
    baseRecords.collect().zipWithIndex.foreach { rowWithInt =>
      val row = rowWithInt._1
      val iter = rowWithInt._2
      row.getInt(0).toString should be("100" + (iter + 1))
      row.getString(1) should be("Val_" + (iter + 1))
      val validFrom = DateTimeFormat
        .forPattern("YYYY-MM-dd HH:mm:ss.SSS")
        .parseDateTime(row.getString(2))
      val validTo = DateTimeFormat
        .forPattern("YYYY-MM-dd HH:mm:ss.SSS")
        .parseDateTime(row.getString(3))
      validFrom.year().get() should be(2017)
      validFrom.monthOfYear().get() should be(1)
      validFrom.dayOfMonth().get() should be(10)
      validTo.year().get() should be(9999)
      validTo.monthOfYear().get() should be(12)
      validTo.dayOfMonth().get() should be(31)
    }

    /**
     * READ CHANGE RECORDS
     */
    And("a change partition with 7 changes")
    And("the changes include 2 inserts, 2 updates and a delete operation")
    val changeRecords =
      reader.read(changeInput, Some(StorageLevel.MEMORY_ONLY))

    /**
     * VALIDATE CHANGE RECORDS
     */
    changeRecords.count() should be(7)
    changeRecords.collect().zipWithIndex.foreach { rowWithInt =>
      val row = rowWithInt._1
      val iter = rowWithInt._2

      row.getString(4) should be(row.getString(6))

      val validTo = DateTimeFormat
        .forPattern("YYYY-MM-dd HH:mm:ss.SSS")
        .parseDateTime(row.getString(6))
      validTo.year().get() should be(2017)
      if (iter < 4) {
        validTo.monthOfYear().get() should be(1)
        validTo.dayOfMonth().get() should be(20)
      } else {
        validTo.monthOfYear().get() should be(2)
        validTo.dayOfMonth().get() should be(15)
      }

      val validFrom = DateTimeFormat
        .forPattern("YYYY-MM-dd HH:mm:ss.SSS")
        .parseDateTime(row.getString(7))
      validFrom.year().get() should be(9999)
      validFrom.monthOfYear().get() should be(12)
      validFrom.dayOfMonth().get() should be(31)

      iter match {
        case 0 =>
          row.getInt(0) should be(1001)
          row.getString(1) should be("0000000000000000000000000707DB59")
          row.getString(2) should be("B")
          row.getString(3) should be("20170126130846670000000000001507041")
          row.getString(5) should be("Val_1")
          row.getBoolean(8) should be(true)
        case 1 =>
          row.getInt(0) should be(1001)
          row.getString(1) should be("0000000000000000000000000707DB59")
          row.getString(2) should be("U")
          row.getString(3) should be("20170126130846670000000000001507041")
          row.getString(5) should be("Val_1_v2")
          row.getBoolean(8) should be(true)
        case 2 =>
          row.getInt(0) should be(1003)
          row.getString(1) should be("0000000000000000000000000707DB59")
          row.getString(2) should be("D")
          row.getString(3) should be("20170126130846670000000000001507043")
          row.getString(5) should be("Val_3")
          row.getBoolean(8) should be(true)
        case 3 =>
          row.getInt(0) should be(1004)
          row.getString(1) should be("0000000000000000000000000707DB59")
          row.getString(2) should be("I")
          row.getString(3) should be("20170126130846670000000000001507044")
          row.getString(5) should be("Val_4")
          row.getBoolean(8) should be(true)
        case 4 =>
          row.getInt(0) should be(1005)
          row.getString(1) should be("0000000000000000000000000707DB61")
          row.getString(2) should be("I")
          row.getString(3) should be("20170126130846670000000000001507045")
          row.getString(5) should be("Val_5")
          row.getBoolean(8) should be(true)
        case 5 =>
          row.getInt(0) should be(1001)
          row.getString(1) should be("0000000000000000000000000707DB61")
          row.getString(2) should be("B")
          row.getString(3) should be("20170126130846670000000000001507046")
          row.getString(5) should be("Val_1_v2")
          row.getBoolean(8) should be(true)
        case 6 =>
          row.getInt(0) should be(1001)
          row.getString(1) should be("0000000000000000000000000707DB61")
          row.getString(2) should be("U")
          row.getString(3) should be("20170126130846670000000000001507046")
          row.getString(5) should be("Val_1_v3")
          row.getBoolean(8) should be(true)
      }
    }

    /**
     * Run the Table processor
     */
    When("the table processor is run")
    tblProcessor.process(properties, reader, writer, userFunctions)

    /**
     * Validate unprocessed changes
     */
    Then("an unprocessed partition should be written")
    And("the three immature records should not be processed")
    And(
      "each of the unprocessed records should have a valid from less than the cutoff")
    val unprocessed = reader.read(properties.immatureChangesOutput, None)
    unprocessed.count() should be(3)
    unprocessed.collect().foreach { row =>
      val formatter = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss.SSS")
      formatter
        .parseDateTime(row.getString(6))
        .isAfter(formatter.parseDateTime(attunityCutoff)) should be(true)
    }

    /**
     * Validate history
     */
    Then("a history partition should be written")
    And("three records should exist in the history")
    val history = reader.read(properties.historyOutput, None)
    history.count() should be(3)
    history.collect.zipWithIndex.foreach { rowWithInt =>
      val row = rowWithInt._1
      val iter = rowWithInt._2
      iter match {
        case 0 =>
          row.getInt(0) should be(1001)
          row.getString(1) should be("Val_1")
          row.getString(2) should be("2017-01-10 12:12:12.121")
          row.getString(3) should be("2017-01-20 10:10:10.100")
          row.getBoolean(4) should be(false)
        case 1 =>
          row.getInt(0) should be(1003)
          row.getString(1) should be("Val_3")
          row.getString(2) should be("2017-01-10 12:12:12.121")
          row.getString(3) should be("2017-01-20 10:10:10.100")
          row.getBoolean(4) should be(false)
        case 2 =>
          row.getInt(0) should be(1003)
          row.getString(1) should be("Val_3")
          row.getString(2) should be("2017-01-20 10:10:10.100")
          row.getString(3) should be("2017-01-20 10:10:10.100")
          row.getBoolean(4) should be(false)
          And("deleted records should open and close at the same instant")
          row.getString(2) should equal(row.getString(3))
      }
    }

    /**
     * Validate active
     */
    Then("an active partition should be written")
    And("three records should exist in the active area")
    And("all records should be before the cuttoff")
    And("all records should be valid to the end of time!")
    val active = reader.read(properties.activeOutput, None)
    active.collect.zipWithIndex.foreach { rowWithInt =>
      val row = rowWithInt._1
      val iter = rowWithInt._2

      val formatter = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss.SSS")
      formatter
        .parseDateTime(row.getString(2))
        .isBefore(formatter.parseDateTime(attunityCutoff)) should be(true)
      row.getString(3) should be("9999-12-31 23:59:59.000")
      iter match {
        case 0 =>
          row.getInt(0) should be(1001)
          row.getString(1) should be("Val_1_v2")
          row.getString(2) should be("2017-01-20 10:10:10.100")
          row.getString(3) should be("9999-12-31 23:59:59.000")
          row.getBoolean(4) should be(true)
        case 1 =>
          row.getInt(0) should be(1002)
          row.getString(1) should be("Val_2")
          row.getString(2) should be("2017-01-10 12:12:12.121")
          row.getString(3) should be("9999-12-31 23:59:59.000")
          row.getBoolean(4) should be(true)
        case 2 =>
          row.getInt(0) should be(1004)
          row.getString(1) should be("Val_4")
          row.getString(2) should be("2017-01-20 10:10:10.100")
          row.getString(3) should be("9999-12-31 23:59:59.000")
          row.getBoolean(4) should be(true)
      }
    }
    active.count should be(3)

    /**
     * Validate metrics
     */
    Then("a metrics record should be written")
    val metrics = reader.read(properties.metricsOutputDir.get, None)
    metrics.collect.foreach { row =>
      println(row)
      And("metrics should show 7 change rows being be read")
      row.getLong(2) should be(7)
      And("metrics should show 3 rows should have been rejected as immature")
      row.getLong(3) should be(3)
      And("metrics should show 4 updates have been processed")
      row.getLong(4) should be(4)
      And("metrics should show 3 distinct transactions were identified")
      row.getLong(5) should be(3)
      And("metrics should show 3 history rows were written")
      row.getLong(6) should be(3)
      And("metrics should show 6 rows were processed in total")
      row.getLong(7) should be(6)
      And("metrics should show 6 were opened or closed")
      row.getLong(8) should be(6)
      And("metrics should show 3 active rows were written")
      row.getLong(9) should be(3)
      And("metrics should show 3 inactive rows were written")
      row.getLong(10) should be(3)
    }
    metrics.count should be(1)
  }

  private def deleteDir(pathString: String) = {
    println(s"deleting prior test path: $pathString")
    val path: Path = Path.fromString(pathString)
    Try(path.deleteRecursively(continueOnFailure = false))
  }
}
