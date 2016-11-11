package com.kainos.enstar.globaldatahub.cdcloader.io

import com.kainos.enstar.globaldatahub.io.{ GDHDataFrameWriter => DFWriter }
import org.apache.hadoop.fs.PathExistsException
import org.apache.spark.sql.{ AnalysisException, DataFrame, SQLContext }
import org.apache.spark.storage.StorageLevel

/**
 * Writes a DataFrame to filesystem.
 */
class CDCDataFrameWriter( writer : DFWriter ) extends DataFrameWriter {

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
      writer.write( sqlContext, path, dataFrame, Some( storageLevel ) )
      dataFrame.count
    } catch {
      //a more readable exception
      case e : AnalysisException => throw new PathExistsException( path )
    }
  }
}
