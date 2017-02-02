package com.kainos.enstar.transformation

case class QueryTestSet( name : String, baseDir : String, querySource : String, queryTests : Set[QueryTest], reconciliationTests : Set[ReconciliationTest] = Set() )
case class QueryTest( name : String, sourceDataSet : Set[CsvSourceData], expectedResult : CsvSourceData, order : List[String] = List() )

case class ReconciliationTest( name : String, sourceDataSet : Set[CsvSourceData], destTableName : String, sourceQuerySource : String, destQuerySource : String )

trait SourceData
case class CsvSourceData( tableName : String, source : String ) extends SourceData
case class QuerySourceData( querySource : String, sourceDataSet : Set[SourceData] ) extends SourceData