package enstar.globaldatahub.cdcloader

import com.kainos.enstar.globaldatahub.cdcloader.module.CDCLoaderModule
import com.kainos.enstar.globaldatahub.cdcloader.processor.CDCSourceProcessor
import com.kainos.enstar.globaldatahub.cdcloader.properties.CDCProperties
import enstar.globaldatahub.cdcloader.module.CDCLoaderModule
import enstar.globaldatahub.cdcloader.processor.CDCSourceProcessor
import enstar.globaldatahub.cdcloader.properties.CDCProperties
import org.apache.spark.sql.SQLContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
 * Main CDCLoader Job
 */
object CDCLoaderJob extends Logging {

  def main( args : Array[String] ) : Unit = {

    logInfo( "CDCLoader starting" )
    val sparkConf = new SparkConf()
    val sparkContext : SparkContext = new SparkContext( sparkConf )
    val sqlContext : SQLContext = new SQLContext( sparkContext )

    val parsedProperties = CDCProperties.parseProperties( args )

    logInfo( "Starting processing!" )
    new CDCSourceProcessor().process( CDCLoaderModule.controlProcessor,
      CDCLoaderModule.properties( parsedProperties ),
      sqlContext,
      CDCLoaderModule.dataFrameReader,
      CDCLoaderModule.dataFrameWriter,
      CDCLoaderModule.tableOperations,
      CDCLoaderModule.tableProcessor,
      CDCLoaderModule.userFunctions,
      CDCLoaderModule.sqlReader )
    logInfo( "Completed processing!" )
  }
}
