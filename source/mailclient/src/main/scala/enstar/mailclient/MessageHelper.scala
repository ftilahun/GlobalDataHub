package enstar.mailclient

import java.util.{ Date, Properties }
import javax.mail.internet.{ AddressException, InternetAddress, MimeMessage }
import javax.mail._

/**
 * Utility object for sending mail messages
 */
object MessageHelper {

  def createMailSession(smtpHost: String,
                        requiresAuth: Boolean,
                        enableTLS: Boolean,
                        port: String = "",
                        username: String = "",
                        password: String = ""): Session = {
    val properties = new Properties()
    properties.put("mail.smtp.host", smtpHost)

    if (enableTLS) {
      if (port.isEmpty) {
        throw new ConnectionDetailsNotSetException("Port is not set for TLS")
      }
      properties.put("mail.smtp.starttls.enable", "true")
      properties.put("mail.smtp.socketFactory.port", port)
      properties.put("mail.smtp.socketFactory.class",
        "javax.net.ssl.SSLSocketFactory")
      properties.put("mail.smtp.socketFactory.fallback", "true")
    }

    if (requiresAuth) {
      if (username.isEmpty) {
        throw new ConnectionDetailsNotSetException("Username not set")
      } else if (password.isEmpty) {
        throw new ConnectionDetailsNotSetException("Password not set")
      } else {
        properties.put("mail.smtp.auth", "true")
        Session.getDefaultInstance(
          properties,
          new javax.mail.Authenticator() {
            override protected def getPasswordAuthentication: PasswordAuthentication = {
              new PasswordAuthentication(username, password)
            }
          }
        )
      }
    } else {
      Session.getDefaultInstance(properties, null)
    }
  }

  /**
   * Implicit methods for message objects
   * @param session an email session
   */
  implicit class SessionExtensions(val session: Session) {

    /**
     * Create a new email
     * @return a mail message
     */
    def composeMessage(): Message = {
      new MimeMessage(session)
    }
  }

  /**
   * Implicit methods for message objects
   * @param message an email message
   */
  implicit class MessageExtensions(val message: Message) {

    /**
     * Add a subject to an email message
     * @param messageSubject the subject to set
     * @return A message with subject set
     */
    def withSubject(messageSubject: String): Message = {
      message.setSubject(messageSubject)
      message
    }

    /**
     * Add a Mutltipart (text/html) content object to a message
     * @param messageContent the content to add
     * @return a message with content set
     */
    def withMultipartBody(messageContent: Multipart): Message = {
      message.setContent(messageContent)
      message
    }

    /**
     * add text content to a mail message
     * @param messageContent the text to add
     * @return a message with text content set
     */
    def withTextBody(messageContent: String): Message = {
      message.setText(messageContent)
      message
    }

    /**
     * Add a collection of To recipients to a mail message
     * @param recipients the recipients of the mail message
     * @throws AddressException on malformed address
     * @throws MessagingException on any other error
     * @return a Message with to recipients set
     */
    @throws[AddressException]
    @throws[MessagingException]
    def withRecipients(recipients: Seq[String]): Message = {
      recipients.foreach(r =>
        addMessageRecipient(message, r, Message.RecipientType.TO))
      message
    }

    /**
     * add a collection of CC recipients to a mail message
     * @param recipients the recipients of the mail message
     * @throws AddressException on malformed address
     * @throws MessagingException on any other error
     * @return a mail message with CC recipients set
     */
    @throws[AddressException]
    @throws[MessagingException]
    def withCCRecipients(recipients: Seq[String]): Message = {
      recipients.foreach(r =>
        addMessageRecipient(message, r, Message.RecipientType.CC))
      message
    }

    /**
     * Add a collection of BCC recipients to a mail message
     * @param recipients the recipients of the mail message
     * @throws AddressException on malformed address
     * @throws MessagingException on any other error
     * @return a mail message with BCC recipients set
     */
    @throws[AddressException]
    @throws[MessagingException]
    def withBCCRecipients(recipients: Seq[String]): Message = {
      recipients.foreach(r =>
        addMessageRecipient(message, r, Message.RecipientType.BCC))
      message
    }

    /**
     * add a single recipient to a mail message
     * @param message the message to add the recipient to
     * @param recipient the recipient to add
     * @param recipientType the type of recipient to add
     */
    private def addMessageRecipient(message: Message,
                                    recipient: String,
                                    recipientType: Message.RecipientType) = {
      message.addRecipient(recipientType, new InternetAddress(recipient))
    }

    /**
     * Add a sender to a mail message
     * @param from the sender
     * @return a mail message with sender set
     */
    def withSender(from: String): Message = {
      message.setFrom(new InternetAddress(from))
      message
    }

    /**
     * Send the mail message
     * @throws AuthenticationFailedException if unable to authenticate
     * @throws MessagingException on error sending mail
     * @return true if sent
     */
    @throws[MessagingException]
    def send(): Boolean = {
      message.setSentDate(new Date())
      Transport.send(message)
      true
    }
  }
}
