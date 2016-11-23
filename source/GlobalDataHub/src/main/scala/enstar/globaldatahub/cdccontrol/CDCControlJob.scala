package enstar.globaldatahub.cdccontrol

import enstar.globaldatahub.cdccontrol.module.CDCControlModule
import enstar.globaldatahub.cdccontrol.properties.ControlProperties
import enstar.globaldatahub.cdccontrol.processor.CDCContolProcessor
import org.apache.spark.{ Logging, SparkConf, SparkContext }
import org.apache.spark.sql.SQLContext

/**
 * Created by ciaranke on 23/11/2016.
 */
object CDCControlJob extends Logging {

  def main( args : Array[String] ) : Unit = {

    logInfo( "CDCLoader starting" )
    val sparkConf = new SparkConf()
    val sparkContext : SparkContext = new SparkContext( sparkConf )
    val sqlContext : SQLContext = new SQLContext( sparkContext )

    val parsedProperties = ControlProperties.parseProperties( args )

    logInfo( "Starting processing!" )
    new CDCContolProcessor().process(
      sqlContext,
      CDCControlModule.dataFrameReader,
      CDCControlModule.dataFrameWriter,
      CDCControlModule.sqlReader,
      CDCControlModule.tableOperations,
      CDCControlModule.properties( parsedProperties )
    )
    logInfo( "Completed processing!" )
  }
}
