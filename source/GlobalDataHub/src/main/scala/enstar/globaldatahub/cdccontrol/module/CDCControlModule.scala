package enstar.globaldatahub.cdccontrol.module

import enstar.globaldatahub.cdccontrol.io.{ ControlDataFrameReader, ControlDataFrameWriter }
import enstar.globaldatahub.cdccontrol.properties.ControlProperties
import enstar.globaldatahub.cdccontrol.udfs.{ ControlUserFunctions, UserFunctions }
import enstar.globaldatahub.common.io._
import enstar.globaldatahub.common.properties.GDHProperties
import org.apache.spark.Logging

/**
 * Creates required objects for this job.
 */
object CDCControlModule extends Logging {

  logInfo( "Creating required objects" )
  private val _dataFrameReader : DataFrameReader = new ControlDataFrameReader(
    new AvroDataFrameReader )
  private val _dataFrameWriter : DataFrameWriter = new ControlDataFrameWriter(
    new AvroDataFrameWriter )
  private val _tableOperations : TableOperations = new DataFrameTableOperations
  private val _sqlReader : SQLReader = new SQLFileReader( new TextFileReader )
  private val _userFunctions = new ControlUserFunctions
  logInfo( "Completed required object creation" )

  /**
   * Get the properties object
   * @param propertyMap a Map of k->v objects
   * @return a properties object
   */
  def properties( propertyMap : Map[String, String] ) : GDHProperties =
    new ControlProperties( propertyMap )

  /**
   * Get the TableOperations
   *
   * @return a TableOperations object
   */
  def tableOperations : TableOperations = _tableOperations
  /**
   * Get an SQLFileReader
   *
   * @return an SQLFileReader
   */
  def sqlReader : SQLReader = _sqlReader

  /**
   * Get the DataFrameWriter
   *
   * @return a DataFrameWriter
   */
  def dataFrameWriter : DataFrameWriter = _dataFrameWriter

  /**
   * Get the DataFrameReader
   *
   * @return a DataFrameReader
   */
  def dataFrameReader : DataFrameReader = _dataFrameReader

  /**
   * Get the UDFs
   *
   * @return a UDF object
   */
  def userFunctions : UserFunctions = _userFunctions

}
