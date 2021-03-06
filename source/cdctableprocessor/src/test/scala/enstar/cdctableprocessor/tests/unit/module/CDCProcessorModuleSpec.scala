package enstar.cdctableprocessor.tests.unit.module

import enstar.cdctableprocessor.io.{ DataFrameReader, DataFrameWriter }
import enstar.cdctableprocessor.module.CDCProcessorModule
import enstar.cdctableprocessor.processor.TableProcessor
import enstar.cdctableprocessor.properties.CDCProperties
import enstar.cdctableprocessor.udfs.UserFunctions
import org.scalatest.{ FlatSpec, GivenWhenThen, Matchers }

/**
 * Unit tests for CDCLoaderModule
 */
class CDCProcessorModuleSpec
    extends FlatSpec
    with GivenWhenThen
    with Matchers {

  "CDCProcessorModule" should "Return correct types" in {
      def typeCheck[T](value: T) = value match {

        case _: DataFrameReader => "DataFrameReader"
        case _: DataFrameWriter => "DataFrameWriter"
        case _: UserFunctions   => "UserFunctions"
        case _: CDCProperties   => "CDCProperties"
        case _: TableProcessor  => "TableProcessor"
      }

    val argsArray = Array[String](
      "--changeInputDir",
      "c",
      "--activeOutput",
      "a",
      "--idColumnName",
      "i",
      "--transactionIdColumnName",
      "t",
      "--changeSequenceColumnName",
      "c",
      "--attunityColumnPrefix",
      "header__",
      "--operationColumnName",
      "B",
      "--operationColumnValueBefore",
      "B",
      "--operationColumnValueInsert",
      "B",
      "--operationColumnValueUpdate",
      "B",
      "--operationColumnValueDelete",
      "B",
      "--validFromColumnName",
      "A",
      "--validToColumnName",
      "A",
      "--transactionTimeStampColumnName",
      "A",
      "--activeColumnName",
      "A",
      "--attunityCutoff",
      "2",
      "--attunityDateFormat",
      "A",
      "--activeInput",
      "A",
      "--immatureChangesOutput",
      "A",
      "--historyOutput",
      "A",
      "--attunityDateFormatShort",
      "A"
    )

    typeCheck(CDCProcessorModule.dataFrameReader) should be("DataFrameReader")
    typeCheck(CDCProcessorModule.dataFrameWriter) should be("DataFrameWriter")
    typeCheck(CDCProcessorModule.userFunctions) should be("UserFunctions")
    typeCheck(CDCProcessorModule.properties(argsArray)) should be(
      "CDCProperties")
    typeCheck(CDCProcessorModule.tableProcessor) should be("TableProcessor")
  }
}
