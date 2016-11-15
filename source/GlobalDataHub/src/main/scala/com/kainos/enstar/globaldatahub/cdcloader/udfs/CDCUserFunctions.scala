package com.kainos.enstar.globaldatahub.cdcloader.udfs

import java.math.BigInteger

import com.kainos.enstar.globaldatahub.properties.GDHProperties
import org.apache.spark.sql.SQLContext
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
 * Created by ciaranke on 14/11/2016.
 */
class CDCUserFunctions extends UserFunctions with Serializable {

  /**
   * Register required UDFs with the SQL context
   *
   * @param sqlContext the sql context
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

  /**
   *
   * @param changeOperation the current change operation.
   * @return true if the chsnge operation is DELETE.
   */
  override def isDeleted( changeOperation : String,
                          properties : GDHProperties ) : java.lang.Boolean =
    changeOperation.equalsIgnoreCase(
      properties.getStringProperty( "DeletedcolumnValue" ) )

  /**
   * * Provides a string representation of the current time in the specified
   * format
   * @param properties properties object
   * @return a string representation of the current timestamp
   */
  def getCurrentTime( properties : GDHProperties ) : String =
    DateTimeFormat
      .forPattern( properties.getStringProperty( "HiveTimeStampFormat" ) )
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
  def isBitSet( bitMask : String, position : Int ) : Boolean = {
    if ( bitMask.length > position ) {
      return bitMask.charAt( position ) == '1'
    }
    false
  }

  /**
   * Generate an attunity change sequence for a table.
   * this sequence should be used when processing the initial load table.
   * @return
   */
  def generateSequenceNumber( properties : GDHProperties ) : String = {
    DateTimeFormat
      .forPattern(
        properties.getStringProperty( "changeSequenceTimestampFormat" ) )
      .print( new DateTime() ) +
      "0000000000000000000"
  }
}
