package enstar.globaldatahub.cdccontrol.udfs

import enstar.globaldatahub.common.properties.GDHProperties
import org.apache.spark.Logging
import org.apache.spark.sql.SQLContext
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
 * Created by ciaranke on 25/11/2016.
 */
class ControlUserFunctions
    extends UserFunctions
    with Serializable
    with Logging {

  /**
   * Register required UDFs with the SQL context
   *
   * @param sqlContext the sql context
   */
  override def registerUDFs(sqlContext: SQLContext,
                            properties: GDHProperties): Unit = {
    logInfo("Registering udfs")
    sqlContext.udf.register("getCurrentTime", () => getCurrentTime(properties))
    logInfo("Completed registering udfs")
  }

  /**
   * * Provides a string representation of the current time in the specified
   * format
   *
   * @param properties properties object
   * @return a string representation of the current timestamp
   */
  override def getCurrentTime(properties: GDHProperties): String =
    DateTimeFormat
      .forPattern(
        properties.getStringProperty("spark.cdcloader.format.timestamp.hive"))
      .print(new DateTime())

}
