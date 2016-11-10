package com.kainos.enstar.globaldatahub.cdcloader.control

import com.kainos.enstar.globaldatahub.cdcloader.io.{ CDCLoaderIO, CDCSQLReaderIO }
import com.kainos.enstar.globaldatahub.cdcloader.properties.CDCProperties
import org.apache.spark.sql.SQLContext
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

class ControlProcessor(
    sqlContext : SQLContext,
    loaderIO : CDCLoaderIO,
    sqlReaderIO : CDCSQLReaderIO,
    properties : CDCProperties ) {

  def registerControlTable : Unit = {
    val controlTableDF = loaderIO.read( sqlContext, properties.controlTablePath, None )
    val name = properties.controlTableName
    loaderIO.registerTempTable( controlTableDF, properties.controlTableName )
  }

  def deregisterControlTable : Unit = loaderIO.deRegisterTempTable( sqlContext, properties.controlTableName )


  def getLastSequenceNumber( tableName : String ) : String = {
    val controlTableSQL = sqlReaderIO.getSQLString( sqlContext.sparkContext, properties.controlTableSQLPath )
    sqlContext.sql(controlTableSQL + tableName).collect()(0).getString(0)
  }

  def generateFirstSequenceNumber : String = {
    DateTimeFormat.forPattern( properties.changeSequenceTimestampFormat ).print( new DateTime() ) +
    "0000000000000000000"
  }

}
