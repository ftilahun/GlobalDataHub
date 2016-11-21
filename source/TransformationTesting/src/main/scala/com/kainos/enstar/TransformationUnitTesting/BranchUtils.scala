package com.kainos.enstar.TransformationUnitTesting

import org.apache.spark.sql.Row

/**
 * Created by terences on 20/11/2016.
 */
object BranchUtils {

  def lookupProfitCentreMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ).toInt, cols( 1 ) )
  }

  def branchMapping( cols : Array[String] ) : Row = cols match {
    case cols if cols.length == 3 => Row( cols( 0 ), cols( 1 ), cols( 2 ), null )
    case _                        => Row( cols( 0 ), cols( 1 ), cols( 2 ), cols( 3 ) )
  }

}
