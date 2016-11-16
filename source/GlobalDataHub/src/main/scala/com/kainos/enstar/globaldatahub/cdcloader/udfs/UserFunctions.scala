package com.kainos.enstar.globaldatahub.cdcloader.udfs

import com.kainos.enstar.globaldatahub.properties.GDHProperties
import org.apache.spark.sql.SQLContext
import com.kainos.enstar.globaldatahub.udfs.{UserFunctions => UDFs}

trait UserFunctions extends UDFs with Serializable {

  /**
    * Register required UDFs with the SQL context
    *
    * @param sqlContext the sql context
    */
  def registerUDFs(sqlContext: SQLContext, properties: GDHProperties): Unit

  /**
    * * Provides a string representation of the current time in the specified
    * format
    * @param properties properties object
    * @return a string representation of the current timestamp
    */
  def getCurrentTime(properties: GDHProperties): String

  /**
    * Converts the attunity change mask from a Hexadecimal string to a binary string
    * @param changeMask the hex string to convert
    * @return a binary string
    */
  def getBitMask(changeMask: String): String

  /**
    * checks if a bit has been set in a binary string
    *
    * @param bitMask the change mask to check
    * @param position the position of the bit in the <b>change table</b>
    * @return true if the bit has been set
    */
  def isBitSet(bitMask: String, position: Integer): java.lang.Boolean

  /** Checks if any bit has been set in a change mask
    *
    * @param changeMask the change mask to check
    * @param columnPositions the position of the columns to check
    * @return true if any bit has been set
    */
  def isAnyBitSet(changeMask: String,
                  columnPositions: Array[String]): java.lang.Boolean

  /**
    *
    * @param changeOperation the current change operation.
    * @param properties properties object
    * @return true if the chsnge operation is DELETE.
    */
  def isDeleted(changeOperation: String,
                properties: GDHProperties): java.lang.Boolean

  /**
    * Generate an attunity change sequence for a table.
    * this sequence should be used when processing the initial load table.
    * @param properties properties object
    * @return
    */
  def generateSequenceNumber(properties: GDHProperties): String
}
