package com.kainos.enstar.transformation

case class QueryTestSet( name : String, baseDir : String, querySource : String, tests : Set[QueryTest] )
case class QueryTest( name : String, sourceDataSet : Set[CsvSourceData], expectedResult : CsvSourceData, order : List[String] = List() )

trait SourceData
case class CsvSourceData( tableName : String, source : String ) extends SourceData
case class QuerySourceData( querySource : String, sourceDataSet : Set[SourceData] ) extends SourceData