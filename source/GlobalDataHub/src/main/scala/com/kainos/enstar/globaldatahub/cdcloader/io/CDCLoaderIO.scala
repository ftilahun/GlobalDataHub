package com.kainos.enstar.globaldatahub.cdcloader.io

import com.kainos.enstar.globaldatahub.io.DataframeIO
import org.apache.hadoop.fs.{ PathExistsException, PathNotFoundException }
import org.apache.hadoop.mapred.InvalidInputException
import org.apache.spark.sql.{ AnalysisException, DataFrame, SQLContext }
import org.apache.spark.storage.StorageLevel

/**
 *
 * @param dataframeIO
 */
class CDCLoaderIO( dataframeIO : DataframeIO ) {

  def read( sqlContext : SQLContext,
            path : String,
            storageLevel : Option[StorageLevel] ) : DataFrame = {
    try {
      dataframeIO.read( sqlContext, path, storageLevel )
    } catch {
      case e : InvalidInputException => throw new PathNotFoundException( path )
    }
  }

  def write(
    sqlContext : SQLContext,
    path : String,
    dataFrame : DataFrame,
    storageLevel : StorageLevel ) : Long = {
    try {
      dataFrame.persist( storageLevel )
      dataframeIO.write( sqlContext, path, dataFrame, Some( storageLevel ) )
      dataFrame.count
    } catch {
      case e : AnalysisException => throw new PathExistsException( path )
    }
  }

  def registerTempTable( dataFrame : DataFrame, tableName : String ) : Unit = {
    dataFrame.registerTempTable( tableName )

  }

  def deRegisterTempTable( sqlContext : SQLContext, tableName : String ) = {
    sqlContext.dropTempTable( tableName )
  }

}
