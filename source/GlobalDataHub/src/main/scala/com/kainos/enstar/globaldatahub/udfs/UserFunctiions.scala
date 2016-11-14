package com.kainos.enstar.globaldatahub.udfs

import org.apache.spark.sql.SQLContext

trait UserFunctiions {

  /** Register required UDFs with the SQL context
    *
    * @param sqlContext the sql context
    */
  def registerUDFs(sqlContext : SQLContext) : Unit
}
