package enstar.mailclient

import org.scalatest.{ FlatSpec, GivenWhenThen, Matchers }
import ContentHelper._

/**
 * Unit tests for ContentHelper
 */
class ContentHelperSpec
    extends FlatSpec
    with GivenWhenThen
    with Matchers {

  "ContentHelper" should "Create text content" in {
    val someText = "SomeText"
    val content = ContentHelper.createMutliPartContent.withTextPart(someText)
    content.getBodyPart(0).getContentType should be ("text/plain")
    content.getBodyPart(0).getContent should be (someText)
  }

  "ContentHelper" should "Create html content" in {
    val html = <html>
                 <body>some text</body>
               </html>
    val content = ContentHelper.createMutliPartContent.withHTMLPart(html)
    content.getBodyPart(0).getContentType should be ("text/plain")
    content.getBodyPart(0).getContent should be (html.toString)

  }

  "ContentHelper" should "Create mixed content" in {
    val someText = "SomeText"
    val html = <html>
                 <body>some text</body>
               </html>

    val content = ContentHelper.createMutliPartContent.withTextPart(someText).withHTMLPart(html)
    content.getBodyPart(0).getContentType should be ("text/plain")
    content.getBodyPart(0).getContent should be (someText)
    content.getBodyPart(1).getContentType should be ("text/plain")
    content.getBodyPart(1).getContent should be (html.toString)
  }
}
