package enstar.globaldatahub.cdcloader.module

import enstar.globaldatahub.cdcloader.control.ControlProcessor
import enstar.globaldatahub.cdcloader.processor.{ SourceProcessor, TableProcessor }
import enstar.globaldatahub.cdcloader.properties.CDCProperties
import enstar.globaldatahub.cdcloader.udfs.UserFunctions
import enstar.globaldatahub.common.io.{ DataFrameReader, DataFrameWriter, SQLReader, TableOperations }
import enstar.globaldatahub.common.properties.GDHProperties
import org.scalatest.{ FlatSpec, GivenWhenThen, Matchers }

/**
 * Unit tests for CDCLoaderModule
 */
class CDCLoaderModuleSpec extends FlatSpec with GivenWhenThen with Matchers {

  "CDCLoaderModuleSpec" should "Return correct tyoes" in {
      def typeCheck[T](value: T) = value match {
        case _: ControlProcessor => "ControlProcessor"
        case _: SourceProcessor  => "SourceProcessor"
        case _: DataFrameReader  => "DataFrameReader"
        case _: DataFrameWriter  => "DataFrameWriter"
        case _: TableOperations  => "TableOperations"
        case _: TableProcessor   => "TableProcessor"
        case _: UserFunctions    => "UserFunctions"
        case _: SQLReader        => "SQLFileReader"
        case _: GDHProperties    => "GDHProperties"
      }

    val args = CDCProperties.parseProperties(Array[String](
      "--cdcOptions",
      "spark.cdcloader.columns.attunity.name.changemask=a," +
        "spark.cdcloader.columns.control.names.controlcolumnnames=a," +
        "spark.cdcloader.columns.attunity.name.changeoperation=a," +
        "spark.cdcloader.columns.attunity.name.changesequence=a," +
        "spark.cdcloader.columns.attunity.value.changeoperation=a," +
        "spark.cdcloader.columns.metadata.name.isdeleted=true," +
        "spark.cdcloader.control.attunity.changetablesuffix=a," +
        "spark.cdcloader.columns.metadata.name.loadtimestamp=a," +
        "spark.cdcloader.path.data.outputbasedir=a," +
        "spark.cdcloader.path.data.outdir=a," +
        "spark.cdcloader.format.timestamp.attunity=a," +
        "spark.cdcloader.format.timestamp.hive=a," +
        "spark.cdcloader.path.data.basedir=a," +
        "spark.cdcloader.path.data.control=a," +
        "spark.cdcloader.path.data.output=a," +
        "spark.cdcloader.path.sql.basedir=a," +
        "spark.cdcloader.path.sql.control=a," +
        "spark.cdcloader.tables.control.name=a," +
        "spark.cdcloader.control.changemask.enabled=a," +
        "spark.cdcloader.input.tablenames=a_b," +
        "spark.cdcloader.control.columnpositions.a=1_2_3_4," +
        "spark.cdcloader.columns.control.name.tablename.a=a," +
        "spark.cdcloader.control.columnpositions.b=1_2_3," +
        "spark.cdcloader.columns.control.name.tablename.b=b"
    ))

    typeCheck(CDCLoaderModule.controlProcessor) should be("ControlProcessor")
    typeCheck(CDCLoaderModule.cdcSourceProcessor) should be("SourceProcessor")
    typeCheck(CDCLoaderModule.dataFrameReader) should be("DataFrameReader")
    typeCheck(CDCLoaderModule.dataFrameWriter) should be("DataFrameWriter")
    typeCheck(CDCLoaderModule.tableOperations) should be("TableOperations")
    typeCheck(CDCLoaderModule.tableProcessor) should be("TableProcessor")
    typeCheck(CDCLoaderModule.userFunctions) should be("UserFunctions")
    typeCheck(CDCLoaderModule.sqlReader) should be("SQLFileReader")
    typeCheck(CDCLoaderModule.properties(args)) should be("GDHProperties")
  }

}
