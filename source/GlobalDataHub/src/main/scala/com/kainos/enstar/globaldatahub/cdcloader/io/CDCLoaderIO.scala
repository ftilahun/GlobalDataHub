package com.kainos.enstar.globaldatahub.cdcloader.io

import com.kainos.enstar.globaldatahub.io.DataframeIO
import org.apache.hadoop.fs.{ PathExistsException, PathNotFoundException }
import org.apache.hadoop.mapred.InvalidInputException
import org.apache.spark.sql.{ AnalysisException, DataFrame, SQLContext }
import org.apache.spark.storage.StorageLevel

/**
 * CDCLoaderIO: reads from filesystem and registers temp tables
 * reading from filesystem is defered to the passed in dataframeIO
 *
 * @param dataframeIO filesystem reader/writer
 */
class CDCLoaderIO( dataframeIO : DataframeIO ) {

  /**
   * Read a DataFrame from the filesystem
   *
   * @param sqlContext the sql context.
   * @param path the path to read from
   * @param storageLevel an optional storagelevel
   * @return a dataframe
   */
  def read( sqlContext : SQLContext,
            path : String,
            storageLevel : Option[StorageLevel] ) : DataFrame = {
    try {
      dataframeIO.read( sqlContext, path, storageLevel )
    } catch {
      //a more readable exception
      case e : InvalidInputException => throw new PathNotFoundException( path )
    }
  }

  /**
   * Write a dataframe to the filesystem.
   *
   * @param sqlContext the sql context
   * @param path the path to write to
   * @param dataFrame the dataframe to write
   * @param storageLevel a storage level to persist at
   * @return a count ot the rows written.
   */
  def write( sqlContext : SQLContext,
             path : String,
             dataFrame : DataFrame,
             storageLevel : StorageLevel ) : Long = {
    try {
      dataframeIO.write( sqlContext, path, dataFrame, Some( storageLevel ) )
      dataFrame.count
    } catch {
      //a more readable exception
      case e : AnalysisException => throw new PathExistsException( path )
    }
  }

  /**
   * Register a temp table over a dataframe
   *
   * @param dataFrame a dataframe
   * @param tableName the tablename
   */
  def registerTempTable( dataFrame : DataFrame, tableName : String ) : Unit = {
    dataFrame.registerTempTable( tableName )

  }

  /**
   * de-register a temp table
   *
   * @param sqlContext the sql context
   * @param tableName the temp table name.
   */
  def deRegisterTempTable( sqlContext : SQLContext, tableName : String ) = {
    sqlContext.dropTempTable( tableName )
  }

}
