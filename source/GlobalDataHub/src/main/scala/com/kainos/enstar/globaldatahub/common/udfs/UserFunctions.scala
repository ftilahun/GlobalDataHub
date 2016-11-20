package com.kainos.enstar.globaldatahub.common.udfs

import com.kainos.enstar.globaldatahub.common.properties.GDHProperties
import org.apache.spark.sql.SQLContext

trait UserFunctions extends Serializable {

  /**
   * Register required UDFs with the SQL context
   *
   * @param sqlContext the sql context
   */
  def registerUDFs( sqlContext : SQLContext, properties : GDHProperties ) : Unit
}
