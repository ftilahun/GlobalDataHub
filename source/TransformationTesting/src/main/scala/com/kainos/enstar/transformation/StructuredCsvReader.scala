package com.kainos.enstar.transformation

import java.net.URL

import org.apache.spark.sql.Row

/**
 * Created by neilri on 16/12/2016.
 */
class StructuredCsvReader( url : URL ) {
  private val csvReader = CsvReader( url )

  val schema : Schema = Schema( csvReader.headers )

  lazy val rows : List[Row] = csvReader.body map schema.stringFieldsToAny map fieldsToRow

  private def fieldsToRow( fields : Array[Any] ) : Row = Row( fields : _* )
}

object StructuredCsvReader {
  def apply( url : URL ) : StructuredCsvReader = new StructuredCsvReader( url )
}