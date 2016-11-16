package com.kainos.enstar.globaldatahub.cdcloader.udfs

import java.math.BigInteger
import com.kainos.enstar.globaldatahub.properties.GDHProperties
import org.apache.spark.sql.SQLContext
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
 * Spark User Defined Functions.
 */
class CDCUserFunctions extends UserFunctions with Serializable {

  /**
   * Register required UDFs with the SQL context
   *
   * @param sqlContext the sql context
   * @param properties the properties object
   */
  override def registerUDFs( sqlContext : SQLContext,
                             properties : GDHProperties ) : Unit = {
    sqlContext.udf.register(
      "isDeleted",
      ( changeOperation : String ) => isDeleted( changeOperation, properties ) )
    sqlContext.udf.register( "checkBitMask",
      ( bitMask : String, position : Int ) =>
        isBitSet( getBitMask( bitMask ), position ) )
    sqlContext.udf.register( "getCurrentTime", () => getCurrentTime( properties ) )
  }

  /** Check if a source row has been deleted.
   *
   * @param changeOperation the current change operation.
   * @param properties the properties object
   * @return true if the chsnge operation is DELETE.
   */
  override def isDeleted( changeOperation : String,
                          properties : GDHProperties ) : java.lang.Boolean =
    changeOperation.equalsIgnoreCase(
      properties.getStringProperty(
        "spark.cdcloader.columns.attunity.value.changeoperation" ) )

  /**
   * * Provides a string representation of the current time in the specified
   * format
   * @param properties properties object
   * @return a string representation of the current timestamp
   */
  def getCurrentTime( properties : GDHProperties ) : String =
    DateTimeFormat
      .forPattern(
        properties.getStringProperty( "spark.cdcloader.format.timestamp.hive" ) )
      .print( new DateTime() )

  /**
   * Converts the attunity change mask from a Hexadecimal string to a binary string
   * @param changeMask the hex string to convert
   * @return a binary string
   */
  def getBitMask( changeMask : String ) : String =
    if ( null != changeMask ) {
      new BigInteger( changeMask, 16 ).toString( 2 ).reverse
    } else {
      "0"
    }

  /**
   * checks if a bit has been set in a binary string
   *
   * @param bitMask the change mask to check
   * @param position the position of the bit in the <b>change table</b>
   * @return true if the bit has been set
   */
  def isBitSet( bitMask : String, position : Integer ) : java.lang.Boolean = {
    if ( bitMask.length > position ) {
      return bitMask.charAt( position ) == '1'
    }
    false
  }

  /**
   * Checks if any bit has been set in a change mask
   *
   * @param changeMask the change mask to check
   * @param columnPositions the position of the columns to check
   * @return true if any bit has been set
   */
  def isAnyBitSet( changeMask : String,
                   columnPositions : Array[String] ) : java.lang.Boolean = {
    val bitMask = getBitMask( changeMask )
    columnPositions.foreach { position =>
      if ( isBitSet( bitMask, position.toInt ) ) {
        return true
      }
    }
    false
  }

  /**
   * Generate an attunity change sequence for a table.
   * this sequence should be used when processing the initial load table.
   * @param properties the properties object
   * @return a string representing a sequence number
   */
  def generateSequenceNumber( properties : GDHProperties ) : String = {
    DateTimeFormat
      .forPattern(
        properties.getStringProperty(
          "spark.cdcloader.format.timestamp.attunity" ) )
      .print( new DateTime() ) +
      "0000000000000000000"
  }
}
