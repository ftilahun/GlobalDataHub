package enstar.globaldatahub.cdccontrol.udfs

import enstar.globaldatahub.TestContexts
import enstar.globaldatahub.cdcloader.udfs.CDCUserFunctions
import enstar.globaldatahub.common.properties.GDHProperties
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeUtils}
import org.mockito.Mockito
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

/**
  * Created by ciaranke on 25/11/2016.
  */
class ControlUserFunctionsSpec extends FlatSpec with GivenWhenThen with Matchers {

  "ControlUserFunctions" should "register UDFs" in {
    Given( "No UDFs are defined" )
    val properties = Mockito.mock( classOf[GDHProperties] )
    Mockito
    .when(
    properties.getStringProperty( "spark.cdcloader.format.timestamp.hive" ) )
    .thenReturn( "YYYY-MM-DD HH:mm:ss.SSS" )
    val userFunctions = new ControlUserFunctions
    Then( "UDfs should be registered" )
    userFunctions.registerUDFs( TestContexts.sqlContext, properties )

    Mockito
    .verify( properties, Mockito.times( 0 ) )
    .getStringProperty( org.mockito.Matchers.anyString() )
    }


  "ControlUserFunctions" should "Return the current time in hive format" in {
    Given( "S date" )
    val properties = Mockito.mock( classOf[GDHProperties] )
    When( "The format is YYYY-MM-DD HH:mm:ss.SSS" )
    Mockito
      .when(
        properties.getStringProperty( "spark.cdcloader.format.timestamp.hive" ) )
      .thenReturn( "YYYY-MM-DD HH:mm:ss.SSS" )
    val userFunctions = new CDCUserFunctions
    val date = new DateTime()
    DateTimeUtils.setCurrentMillisFixed( date.getMillis )
    Then( "The date should me formated to match" )
    userFunctions.getCurrentTime( properties ) should
      be(
        DateTimeFormat.forPattern( "YYYY-MM-DD HH:mm:ss.SSS" ).print( date )
      )
    DateTimeUtils.setCurrentMillisSystem()

    Mockito
      .verify( properties, Mockito.times( 1 ) )
      .getStringProperty( org.mockito.Matchers.anyString() )
  }
}
