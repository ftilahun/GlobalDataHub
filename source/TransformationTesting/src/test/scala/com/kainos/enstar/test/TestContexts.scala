package com.kainos.enstar.test

import org.apache.spark.sql.SQLContext
import org.apache.spark.{ SparkConf, SparkContext }

/**
 * Created by terences on 20/11/2016.
 */
object TestContexts {

  private val _sc : SparkContext = new SparkContext(
    new SparkConf()
      .setMaster( "local[1]" )
      .setAppName( this.getClass.getSimpleName ) )
  _sc.setLogLevel( "OFF" )
  private val _sqlC : SQLContext = new SQLContext( _sc )

  def sparkContext = _sc
  def sqlContext = _sqlC

}
