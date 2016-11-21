package com.kainos.enstar.globaldatahub.cdcloader.instanciator

import com.kainos.enstar.globaldatahub.cdcloader.control.ControlProcessor
import com.kainos.enstar.globaldatahub.cdcloader.io.SQLFileReader
import com.kainos.enstar.globaldatahub.cdcloader.processor.{SourceProcessor, TableProcessor}
import com.kainos.enstar.globaldatahub.cdcloader.udfs.UserFunctions
import com.kainos.enstar.globaldatahub.common.io.{DataFrameReader, DataFrameWriter, TableOperations}
import com.kainos.enstar.globaldatahub.common.properties.GDHProperties
import org.apache.spark.sql.SQLContext
import org.mockito.Mockito
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

/**
 * Unit tests for CDCLoaderInstanciator
 */
class CDCLoaderInstanciatorSpec
    extends FlatSpec
    with GivenWhenThen
    with Matchers {

  "CDCLoaderInstanciatorSpec" should "Return correct tyoes" in {
      def typeCheck[T]( value : T ) = value match {
        case _ : ControlProcessor => "ControlProcessor"
        case _ : SourceProcessor  => "SourceProcessor"
        case _ : DataFrameReader  => "DataFrameReader"
        case _ : DataFrameWriter  => "DataFrameWriter"
        case _ : TableOperations  => "TableOperations"
        case _ : TableProcessor   => "TableProcessor"
        case _ : UserFunctions    => "UserFunctions"
        case _ : SQLFileReader    => "SQLFileReader"
        case _ : GDHProperties    => "GDHProperties"
      }

    val args = Array[String](
      "--cdcOptions",
      "spark.cdcloader.columns.attunity.name.changemask=a," +
        "spark.cdcloader.columns.control.names.controlcolumnnames=a," +
        "spark.cdcloader.columns.attunity.name.changeoperation=a," +
        "spark.cdcloader.columns.attunity.name.changesequence=a," +
        "spark.cdcloader.columns.attunity.value.changeoperation=a," +
        "spark.cdcloader.columns.metadata.name.isdeleted=true," +
        "spark.cdcloader.control.attunity.changetablesuffix=a," +
        "spark.cdcloader.columns.metadata.name.loadtimestamp=a," +
        "spark.cdcloader.paths.data.outputbasedir=a," +
        "spark.cdcloader.paths.data.outdir=a," +
        "spark.cdcloader.format.timestamp.attunity=a," +
        "spark.cdcloader.format.timestamp.hive=a," +
        "spark.cdcloader.paths.data.basedir=a," +
        "spark.cdcloader.paths.data.control=a," +
        "spark.cdcloader.paths.data.output=a," +
        "spark.cdcloader.paths.sql.basedir=a," +
        "spark.cdcloader.paths.sql.control=a," +
        "spark.cdcloader.tables.control.name=a," +
        "spark.cdcloader.control.changemask.enabled=a," +
        "spark.cdcloader.input.tablenames=a_b," +
        "spark.cdcloader.control.columnpositions.a=1_2_3_4," +
        "spark.cdcloader.columns.control.name.tablename.a=a," +
        "spark.cdcloader.control.columnpositions.b=1_2_3," +
        "spark.cdcloader.columns.control.name.tablename.b=b"
    )

    typeCheck( CDCLoaderInstanciator.controlProcessor ) should be(
      "ControlProcessor" )
    typeCheck( CDCLoaderInstanciator.cdcSourceProcessor ) should be(
      "SourceProcessor" )
    typeCheck( CDCLoaderInstanciator.dataFrameReader ) should be(
      "DataFrameReader" )
    typeCheck( CDCLoaderInstanciator.dataFrameWriter ) should be(
      "DataFrameWriter" )
    typeCheck( CDCLoaderInstanciator.tableOperations ) should be(
      "TableOperations" )
    typeCheck( CDCLoaderInstanciator.tableProcessor ) should be( "TableProcessor" )
    typeCheck( CDCLoaderInstanciator.userFunctions ) should be( "UserFunctions" )
    typeCheck( CDCLoaderInstanciator.sqlReader ) should be( "SQLFileReader" )
    typeCheck( CDCLoaderInstanciator.properties( args ) ) should be(
      "GDHProperties" )
  }

}
