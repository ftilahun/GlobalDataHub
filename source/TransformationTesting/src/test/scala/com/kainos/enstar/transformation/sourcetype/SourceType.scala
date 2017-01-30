package com.kainos.enstar.transformation.sourcetype

import com.kainos.enstar.transformation.tags
import org.scalatest.Tag

/**
 * Created by neilri on 16/01/2017.
 */
sealed abstract class SourceType( val packageName : String, val testTags : List[Tag] = List() )

case object Ndex extends SourceType( "ndex", testTags = List( tags.Ndex ) )

case object Genius extends SourceType( "genius", testTags = List( tags.Genius ) )
