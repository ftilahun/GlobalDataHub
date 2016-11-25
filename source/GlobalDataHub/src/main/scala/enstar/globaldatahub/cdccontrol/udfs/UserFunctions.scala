package enstar.globaldatahub.cdccontrol.udfs

import enstar.globaldatahub.common.properties.GDHProperties
import enstar.globaldatahub.common.udfs.{ UserFunctions => UDFs }

/**
 * Created by ciaranke on 25/11/2016.
 */
trait UserFunctions extends UDFs with Serializable {

  /**
   * * Provides a string representation of the current time in the specified
   * format
   * @param properties properties object
   * @return a string representation of the current timestamp
   */
  def getCurrentTime( properties : GDHProperties ) : String
}
