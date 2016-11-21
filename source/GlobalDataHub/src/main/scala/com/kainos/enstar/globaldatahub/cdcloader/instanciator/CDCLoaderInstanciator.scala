package com.kainos.enstar.globaldatahub.cdcloader.instanciator

import com.kainos.enstar.globaldatahub.cdcloader.control.{CDCControlProcessor, ControlProcessor}
import com.kainos.enstar.globaldatahub.cdcloader.io._
import com.kainos.enstar.globaldatahub.cdcloader.processor.{CDCSourceProcessor, CDCTableProcessor, SourceProcessor, TableProcessor}
import com.kainos.enstar.globaldatahub.cdcloader.properties.CDCProperties
import com.kainos.enstar.globaldatahub.cdcloader.udfs.{CDCUserFunctions, UserFunctions}
import com.kainos.enstar.globaldatahub.common.io._
import com.kainos.enstar.globaldatahub.common.properties.GDHProperties
import org.apache.spark.Logging

/**
 * Creates required objects for this job.
 */
object CDCLoaderInstanciator extends Logging {

  logInfo( "Creating required objects" )
  private val _controlProcessor : ControlProcessor = new CDCControlProcessor
  private val _cdcSourceProcessor : SourceProcessor = new CDCSourceProcessor
  private val _dataFrameReader : DataFrameReader = new CDCDataFrameReader(
    new AvroDataFrameReader )
  private val _dataFrameWriter : DataFrameWriter = new CDCDataFrameWriter(
    new AvroDataFrameWriter )
  private val _tableOperations : TableOperations = new DataFrameTableOperations
  private val _tableProcessor : TableProcessor = new CDCTableProcessor
  private val _userFunctions : UserFunctions = new CDCUserFunctions
  private val _sqlReader : SQLFileReader = new CDCSQLFileReader(
    new TextFileReader )
  logInfo( "Complete!" )

  /**
   * Get the ControlProcessor
   *
   * @return a ControlProcessor
   */
  def controlProcessor : ControlProcessor = _controlProcessor

  /**
   * Get the SourceProcessor
   *
   * @return a SourceProcessor
   */
  def cdcSourceProcessor : SourceProcessor = _cdcSourceProcessor

  /**
   * Get the DataFrameReader
   *
   * @return a DataFrameReader
   */
  def dataFrameReader : DataFrameReader = _dataFrameReader

  /**
   * Get the DataFrameWriter
   *
   * @return a DataFrameWriter
   */
  def dataFrameWriter : DataFrameWriter = _dataFrameWriter

  /**
   * Get the TableOperations
   *
   * @return a TableOperations object
   */
  def tableOperations : TableOperations = _tableOperations

  /**
   * Get the TableProcessor
   *
   * @return a TableProcessor
   */
  def tableProcessor : TableProcessor = _tableProcessor

  /**
   * Get a UserFunctions object
   *
   * @return a UserFunctions object
   */
  def userFunctions : UserFunctions = _userFunctions

  /**
   * Get an SQLFileReader
   *
   * @return an SQLFileReader
   */
  def sqlReader : SQLFileReader = _sqlReader

  /**
   * Get the properties object
   * @param args The command line arguments
   * @return a properties object
   */
  def properties( args : Array[String] ) : GDHProperties =
    new CDCProperties( CDCProperties.parseProperties( args ) )

}
