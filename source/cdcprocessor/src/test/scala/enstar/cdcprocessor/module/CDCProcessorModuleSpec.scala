package enstar.cdcprocessor.module

import enstar.cdcprocessor.io.{ DataFrameReader, DataFrameWriter }
import enstar.cdcprocessor.processor.TableProcessor
import enstar.cdcprocessor.properties.CDCProperties
import enstar.cdcprocessor.udfs.UserFunctions
import org.scalatest.{ FlatSpec, GivenWhenThen, Matchers }

/**
 * Unit tests for CDCLoaderModule
 */
class CDCProcessorModuleSpec
    extends FlatSpec
    with GivenWhenThen
    with Matchers {

  "CDCLoaderModuleSpec" should "Return correct tyoes" in {
      def typeCheck[T](value: T) = value match {

        case _: DataFrameReader => "DataFrameReader"
        case _: DataFrameWriter => "DataFrameWriter"
        case _: UserFunctions   => "UserFunctions"
        case _: CDCProperties   => "CDCProperties"
        case _: TableProcessor  => "TableProcessor"
      }

    val argsArray = Array[String](
      "--changeInput",
      "c",
      "--activeOutput",
      "a",
      "--idColumnName",
      "i",
      "--transactionColumnName",
      "t",
      "--changeSequenceColumnName",
      "c",
      "--attunityColumnPrefix",
      "header__"
    )

    typeCheck(CDCProcessorModule.dataFrameReader) should be("DataFrameReader")
    typeCheck(CDCProcessorModule.dataFrameWriter) should be("DataFrameWriter")
    typeCheck(CDCProcessorModule.userFunctions) should be("UserFunctions")
    typeCheck(CDCProcessorModule.properties(argsArray)) should be(
      "CDCProperties")
    typeCheck(CDCProcessorModule.tableProcessor) should be("TableProcessor")
  }
}
