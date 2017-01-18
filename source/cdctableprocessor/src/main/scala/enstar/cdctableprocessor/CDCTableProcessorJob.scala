package enstar.cdctableprocessor

import enstar.cdctableprocessor.module.CDCProcessorModule
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{ Logging, SparkConf, SparkContext }

/**
 * Main class for CDCTableProcessor
 */
object CDCTableProcessorJob extends Logging {

  def main(args: Array[String]): Unit = {

    logInfo("Parsing properties")
    val properties = CDCProcessorModule.properties(args)

    val reader = CDCProcessorModule.dataFrameReader
    val writer = CDCProcessorModule.dataFrameWriter
    val userFunctions = CDCProcessorModule.userFunctions

    logInfo("Creating contexts")
    val sparkConf = new SparkConf()
    val sparkContext = new SparkContext(sparkConf)
    //hive context required for Window functions
    val sqlContext = new HiveContext(sparkContext)

    val tableProcessor = CDCProcessorModule.tableProcessor

    logInfo("Processing table")
    tableProcessor
      .process(sqlContext, properties, reader, writer, userFunctions)
    logInfo("Done!")
  }
}
