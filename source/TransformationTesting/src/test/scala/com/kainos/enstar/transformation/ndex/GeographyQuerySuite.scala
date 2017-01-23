package com.kainos.enstar.transformation.ndex

import com.kainos.enstar.transformation._
import com.kainos.enstar.transformation.sourcetype.{ Ndex, SourceType }

/**
 * Created by neilri on 16/01/2017.
 */
class GeographyQuerySuite extends QuerySuite {
  override val sourceType : SourceType = Ndex

  override def testTags = List( tags.Geography )

  override def queryTestSets : List[QueryTestSet] = List(
    QueryTestSet(
      "Geography",
      "geography",
      "Geography.hql",
      Set(
        QueryTest(
          "Primary",
          Set(
            CsvSourceData( "lookup_country", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "geography", "PrimaryTestData.csv" )
        )
      )
    )
  )
}
