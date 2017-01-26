package enstar.mailclient
import javax.mail.internet.{ MimeBodyPart, MimeMultipart }
import scala.xml.Elem

/**
 * Helper class for creating multipart emails
 */
object ContentHelper {

  /**
   * Create a new MimeMultipart message content object
   * @return
   */
  def createMutliPartContent: MimeMultipart = {
    new MimeMultipart("alternative")
  }

  /**
   * Implicit methods on MimeMultipart objects
   * @param mimeMultipart
   */
  implicit class ContentExtensions(val mimeMultipart: MimeMultipart) {

    /**
     * add a text part to a MimeMultipart object
     * @param text the text to add
     * @return a MimeMultipart object
     */
    def withTextPart(text: String): MimeMultipart = {
      val textPart = new MimeBodyPart()
      textPart.setText(text, "utf-8")
      mimeMultipart.addBodyPart(textPart)
      mimeMultipart
    }

    /**
     * add a HTML part to a MimeMultipart object
     * @param html the html to add
     * @return a MimeMultipart object
     */
    def withHTMLPart(html: Elem): MimeMultipart = {
      val htmlPart = new MimeBodyPart()
      htmlPart.setContent(html.toString, "text/html; charset=utf-8")
      mimeMultipart.addBodyPart(htmlPart)
      mimeMultipart
    }
  }
}
