package com.kainos.enstar.TransformationUnitTesting

import org.apache.spark.sql.Row

/**
 * Created by terences on 21/11/2016.
 */
object LegalEntityUtils {

  def lookupProfitCentreMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ).toInt, cols( 1 ) )
  }

  def legalEntityMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ), cols( 1 ), cols( 2 ), null, null )
  }
}
