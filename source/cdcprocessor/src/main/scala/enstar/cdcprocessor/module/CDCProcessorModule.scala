package enstar.cdcprocessor.module

import enstar.cdcprocessor.io._
import enstar.cdcprocessor.processor.{ CDCTableProcessor, TableProcessor }
import enstar.cdcprocessor.properties.{ CDCProperties, CommandLinePropertyParser }
import enstar.cdcprocessor.udfs.{ CDCUserFunctions, UserFunctions }
import org.apache.spark.Logging

/**
 * Creates required objects for this job.
 */
object CDCProcessorModule extends Logging {
  logInfo("Creating required objects")
  private val _dataFrameReader: DataFrameReader = new CDCDataFrameReader(
    new AvroDataFrameReader)
  private val _dataFrameWriter: DataFrameWriter = new CDCDataFrameWriter(
    new AvroDataFrameWriter)
  private val _userFunctions: UserFunctions = new CDCUserFunctions
  private val _tableProcessor: TableProcessor = new CDCTableProcessor
  logInfo("Completed required object creation")

  /**
   * Get the DataFrameReader
   *
   * @return a DataFrameReader
   */
  def dataFrameReader: DataFrameReader = _dataFrameReader

  /**
   * Get the DataFrameWriter
   *
   * @return a DataFrameWriter
   */
  def dataFrameWriter: DataFrameWriter = _dataFrameWriter

  /**
   * Get a UserFunctions object
   *
   * @return a UserFunctions object
   */
  def userFunctions: UserFunctions = _userFunctions

  /**
   * Get the TableProcessor
   *
   * @return a TableProcessor
   */
  def tableProcessor: TableProcessor = _tableProcessor

  /**
   * Get the properties object
   * @param args command line arguments
   * @return a properties object
   */
  def properties(args: Array[String]): CDCProperties =
    new CommandLinePropertyParser().parse(args)
}
