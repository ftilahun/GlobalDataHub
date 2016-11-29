package enstar.globaldatahub.cdccontrol.module

import enstar.globaldatahub.cdccontrol.properties.ControlProperties
import enstar.globaldatahub.common.io.{ DataFrameReader, DataFrameWriter, SQLReader, TableOperations }
import enstar.globaldatahub.common.properties.GDHProperties
import org.scalatest.{ FlatSpec, GivenWhenThen, Matchers }

/**
 * Unit tests for CDCControlModule
 */
class CDCControlModuleSpec
    extends FlatSpec
    with GivenWhenThen
    with Matchers {
  "CDCControlModule" should "Return correct types" in {
      def typeCheck[T](value: T) = value match {
        case _: DataFrameReader => "DataFrameReader"
        case _: DataFrameWriter => "DataFrameWriter"
        case _: TableOperations => "TableOperations"
        case _: SQLReader       => "SQLFileReader"
        case _: GDHProperties   => "GDHProperties"
      }

    val args = ControlProperties.parseProperties(Array[String](
      "--ctrlOptions",
      "spark.cdccontrol.path.sql=a," +
        "spark.cdccontrol.path.data.control.input=b," +
        "spark.cdccontrol.path.data.control.output=c," +
        "spark.cdccontrol.path.data.input=d," +
        "spark.cdccontrol.tables.control.name=e," +
        "spark.cdccontrol.tables.temp.name=f"
    ))

    typeCheck(CDCControlModule.dataFrameReader) should be("DataFrameReader")
    typeCheck(CDCControlModule.dataFrameWriter) should be("DataFrameWriter")
    typeCheck(CDCControlModule.tableOperations) should be("TableOperations")
    typeCheck(CDCControlModule.sqlReader) should be("SQLFileReader")
    typeCheck(CDCControlModule.properties(args)) should be("GDHProperties")
  }
}
