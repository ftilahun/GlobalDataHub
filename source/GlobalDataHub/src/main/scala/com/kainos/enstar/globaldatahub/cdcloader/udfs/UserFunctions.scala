package com.kainos.enstar.globaldatahub.cdcloader.udfs

import java.math.BigInteger

import org.apache.spark.sql.SQLContext
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import com.kainos.enstar.globaldatahub.udfs.{ UserFunctions => UDFs }

trait UserFunctions extends UDFs {

  /**
   * Register required UDFs with the SQL context
   *
   * @param sqlContext the sql context
   */
  def registerUDFs( sqlContext : SQLContext ) : Unit

  /**
   * * Provides a string representation of the current time in the specified
   * format
   * @param format the format for the timestamp
   * @return a string representation of the current timestamp
   */
  def getCurrentTime( format : String ) : String
  /**
   * Converts the attunity change mask from a Hexadecimal string to a binary string
   * @param changeMask the hex string to convert
   * @return a binary string
   */
  def getBitMask( changeMask : String ) : String

  /**
   * checks if a bit has been set in a binary string
   *
   * @param bitMask the change mask to check
   * @param position the position of the bit in the <b>change table</b>
   * @return true if the bit has been set
   */
  def isBitSet( bitMask : String, position : Int ) : Boolean

  /**
   *
   * @param changeOperation the current change operation.
   * @return true if the chsnge operation is DELETE.
   */
  def isDeleted( changeOperation : String ) : Boolean

  /**
   * Generate an attunity change sequence for a table.
   * this sequence should be used when processing the initial load table.
   * @return
   */
  def generateSequenceNumber : String

}
