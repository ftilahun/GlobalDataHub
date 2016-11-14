package com.kainos.enstar.globaldatahub.cdcloader.udfs

import com.kainos.enstar.globaldatahub.TestContexts
import com.kainos.enstar.globaldatahub.properties.GDHProperties
import org.joda.time.{DateTime, DateTimeUtils}
import org.joda.time.format.DateTimeFormat
import org.mockito.Mockito
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class CDCUserFunctionsSpec extends FlatSpec
  with GivenWhenThen
  with Matchers {

  "CDCUserFunctions" should "register UDFs" in {
    val properties = Mockito.mock(classOf[GDHProperties])
    Mockito.when(properties.getStringProperty("HiveTimeStampFormat")).
      thenReturn("YYYY-MM-DD HH:mm:ss.SSS")
    val userFunctions = new CDCUserFunctions(properties)
    userFunctions.registerUDFs(TestContexts.sqlContext)
  }

  "CDCUserFunctions" should "Return the current time in hive format" in {
    val properties = Mockito.mock(classOf[GDHProperties])
    Mockito.when(properties.getStringProperty("HiveTimeStampFormat")).thenReturn("YYYY-MM-DD HH:mm:ss.SSS")
    val userFunctions = new CDCUserFunctions(properties)
    val date = new DateTime()
    DateTimeUtils.setCurrentMillisFixed( date.getMillis )
    userFunctions.getCurrentTime(properties.getStringProperty("HiveTimeStampFormat")) should
      be (
        DateTimeFormat.forPattern("YYYY-MM-DD HH:mm:ss.SSS").print(date)
      )
    DateTimeUtils.currentTimeMillis()
    DateTimeUtils.setCurrentMillisSystem()
  }

  "CDCUserFunctions" should "Check if a row is deleted" in {
    val properties = Mockito.mock(classOf[GDHProperties])
    val userFunctions = new CDCUserFunctions(properties)
    Mockito.when(properties.getStringProperty("DeletedcolumnValue")).thenReturn("DELETE")
    userFunctions.isDeleted("DELETE") should be (true)
    userFunctions.isDeleted("INSERT") should be (false)
  }

  "CDCUserFunctions" should "Check if a row is filtered out" in {
    val properties = Mockito.mock(classOf[GDHProperties])
    val userFunctions = new CDCUserFunctions(properties)

  }

  "CDCUserFunctions" should "covert the change mask to a bit mask correctly" in {
    val properties = Mockito.mock(classOf[GDHProperties])
    val userFunctions = new CDCUserFunctions(properties)
    userFunctions.getBitMask("0380") should equal("0000000111")
  }

  "CDCUserFunctions" should "identify bits that have been set correctly" in {
    val properties = Mockito.mock(classOf[GDHProperties])
    val userFunctions = new CDCUserFunctions(properties)
    userFunctions.isBitSet(userFunctions.getBitMask("380"),9) should be (true)
    userFunctions.isBitSet(userFunctions.getBitMask("380"),10) should be (false)
  }
}
