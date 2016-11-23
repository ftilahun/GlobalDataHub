package enstar.globaldatahub.cdcloader.control

import com.kainos.enstar.globaldatahub.common.io.{DataFrameReader, SQLReader, TableOperations}
import com.kainos.enstar.globaldatahub.common.properties.GDHProperties
import org.apache.spark.sql.SQLContext
import com.kainos.enstar.globaldatahub.common.processor.{ControlProcessor => CommonControlProcessor}
import enstar.globaldatahub.common.io.{DataFrameReader, SQLReader, TableOperations}
import enstar.globaldatahub.common.processor
import enstar.globaldatahub.common.processor.ControlProcessor
import enstar.globaldatahub.common.properties.GDHProperties

/**
 * Defines expected Behaviour for a control processor.
 */
trait ControlProcessor extends processor.ControlProcessor {

  /**
   * Checks if a table has been previously processed
   *
   * @param sqlContext the SQL Context
   * @param tableName the table to check
   * @param properties properties file
   * @return true if previously processed
   */
  def isInitialLoad( sqlContext : SQLContext,
                     tableName : String,
                     properties : GDHProperties ) : Boolean

  /**
   * Register a control table for the source system
   * @param sqlContext the SQL Context
   * @param reader CDCDataFrameReader, for reading from a filesystem
   * @param properties properties file
   * @param tableOperation table operations object, for registering tables
   */
  def registerControlTable( sqlContext : SQLContext,
                            reader : DataFrameReader,
                            properties : GDHProperties,
                            tableOperation : TableOperations ) : Unit

  /**
   * De-register the control table for a source system
   * @param sqlContext the SQL Context
   * @param properties properties file
   * @param tableOperation table operations object, for registering tables
   */
  def deregisterControlTable( sqlContext : SQLContext,
                              properties : GDHProperties,
                              tableOperation : TableOperations ) : Unit

  /**
   * get the last attunity change sequence in the control table the sql expected by this method is
   * as follows:
   *
   * SELECT MAX(lastattunitychangeseq) FROM control WHERE attunitytablename =
   *
   * @param sqlContext the SQL Context
   * @param sqlFileReader SQL reader for reading SQL files from the filesystem
   * @param properties properties file
   * @param tableName the name of the source table being processed
   * @return an attunity change sequence
   */
  def getLastSequenceNumber( sqlContext : SQLContext,
                             sqlFileReader : SQLReader,
                             properties : GDHProperties,
                             tableName : String ) : String
}
