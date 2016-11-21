package com.kainos.enstar.globaldatahub.cdcloader

import com.kainos.enstar.globaldatahub.cdcloader.instanciator.CDCLoaderInstanciator
import com.kainos.enstar.globaldatahub.cdcloader.processor.CDCSourceProcessor
import org.apache.spark.sql.SQLContext
import org.apache.spark.{ Logging, SparkConf, SparkContext }

/**
 * Main CDCLoader Job
 */
object CDCLoaderJob extends Logging {

  def main( args : Array[String] ) : Unit = {

    logInfo( "CDCLoader starting" )
    val sparkConf = new SparkConf()
    val sparkContext : SparkContext = new SparkContext( sparkConf )
    val sqlContext : SQLContext = new SQLContext( sparkContext )

    logInfo( "Starting processing!" )
    new CDCSourceProcessor().process( CDCLoaderInstanciator.controlProcessor,
      CDCLoaderInstanciator.properties( args ),
      sqlContext,
      CDCLoaderInstanciator.dataFrameReader,
      CDCLoaderInstanciator.dataFrameWriter,
      CDCLoaderInstanciator.tableOperations,
      CDCLoaderInstanciator.tableProcessor,
      CDCLoaderInstanciator.userFunctions,
      CDCLoaderInstanciator.sqlReader )
    logInfo( "Completed processing!" )
  }
}
