package com.kainos.enstar.globaldatahub.cdcloader.properties

import com.kainos.enstar.globaldatahub.properties.DatahubProperties

class CDCProperties extends DatahubProperties {


  override def getBoolenProperty(s: String): Boolean = ???

  override def checkPropertiesSet : Unit = ???

  override def getStringProperty(name: String): String = ???

  override def getArrayProperty(name: String): Array[String] = ???
}
