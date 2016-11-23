package com.kainos.enstar.TransformationUnitTesting

import java.io.InputStream

import org.apache.spark.sql.{ DataFrame, SQLContext }

/**
 * Created by terences on 20/11/2016.
 */
object SQLRunner {

  def runStatement( statement : String, sqlContext : SQLContext ) : DataFrame = {
    sqlContext.sql( statement )
  }

}
