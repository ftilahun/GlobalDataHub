package com.kainos.enstar.globaldatahub.cdcloader.properties

import java.lang.Boolean

import com.kainos.enstar.globaldatahub.properties.GDHProperties

class CDCProperties extends GDHProperties {

  override def getBooleanProperty(s: String): java.lang.Boolean = ???

  override def checkPropertiesSet : Unit = ???

  override def getStringProperty( name : String ) : String = ???

  override def getArrayProperty( name : String ) : Array[String] = ???
}
