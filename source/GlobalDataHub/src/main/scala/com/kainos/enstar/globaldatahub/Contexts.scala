package com.kainos.enstar.globaldatahub

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by ciaranke on 10/11/2016.
 */
object Contexts {
  private val _sc : SparkContext = new SparkContext( new SparkConf() )
  private val _sqlC : SQLContext = new SQLContext( _sc )

  def sparkContext = _sc
  def sqlContext = _sqlC
}
