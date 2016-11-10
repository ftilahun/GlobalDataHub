package com.kainos.enstar.globaldatahub.properties

/**
 * Trait for reading property values.
 */
trait DatahubProperties {

  def getBoolenProperty( s : String ) : Boolean = ???

  def getStringProperty( name : String ) : String = ???

  def getArrayProperty( name : String ) : Array[String] = ???

  def checkPropertiesSet : Unit = ???
}
