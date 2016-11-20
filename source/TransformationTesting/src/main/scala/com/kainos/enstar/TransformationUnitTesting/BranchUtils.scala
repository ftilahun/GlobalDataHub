package com.kainos.enstar.TransformationUnitTesting

import org.apache.spark.sql.Row

/**
 * Created by terences on 20/11/2016.
 */
object BranchUtils {

  def lookupProfitCentreMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ).toInt, cols( 1 ) )
  }

  def branchMapping( cols : Array[String] ) : Row = {
    if ( cols.size < 4 ) {
      Row( cols( 0 ), cols( 1 ), cols( 2 ), "" )
    } else {
      Row( cols( 0 ), cols( 1 ), cols( 2 ), cols( 3 ) )
    }
  }

}
