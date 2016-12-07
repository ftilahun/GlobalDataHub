package com.kainos.enstar.TransformationUnitTesting

import org.apache.spark.sql.Row

/**
 * Created by caoimheb on 07/12/2016.
 */
object TrustFundUtils {

  def lookupTrustFundMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ), cols( 1 ) )
  }

  def trustFundMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ), cols( 1 ), cols( 2 ) )
  }

}
