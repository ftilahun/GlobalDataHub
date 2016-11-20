package com.kainos.enstar.testing

import org.apache.spark.sql.Row

/**
 * Created by terences on 20/11/2016.
 */
object BranchUtils {

  def lookupProfitCentreMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ).toInt, cols( 1 ) )
  }

  def branchMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ) )
  }

}
