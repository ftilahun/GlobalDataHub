package enstar.mailclient

import java.util.Properties
import javax.mail.internet.AddressException
import javax.mail.{ Folder, Message, Session }

import org.scalatest.{ FlatSpec, GivenWhenThen, Matchers }
import MessageHelper._
import de.saly.javamail.mock2.MockMailbox

/**
 * Unit tests for MessageHelper
 */
class MessageHelperSpec
    extends FlatSpec
    with GivenWhenThen
    with Matchers {

  MockMailbox.resetAll()

  "MessageHelper" should "Compose an email correctly" in {

    val props = new Properties()
    val session = Session.getInstance(props)
    val subject = "TestSubject"
    val content = "MessageContext"
    val sender = "c.kearney@kainos.com"
    val toList = List("to@kainos.com", "to@kainos.com", "to@kainos.com")
    val invalidToList = List("Hello there")
    val ccList = List("cc@kainos.com", "cc@kainos.com", "cc@kainos.com")
    val bccList = List("bcc@kainos.com", "bcc@kainos.com", "bcc@kainos.com")

    Given("A call to createMessage with the subject " + subject)
    And("The content is " + content)
    And("The message is from " + sender)
    And("A To list of " + toList.mkString(","))
    And("A CC list of " + ccList.mkString(","))
    And("A BCC list of " + bccList.mkString(","))

    When("The method is called")
    val correctMessage = session.composeMessage().
      withSubject(subject).
      withTextBody(content).
      withRecipients(toList).
      withCCRecipients(ccList).
      withBCCRecipients(bccList).withSender(sender)

    Then("A Message should be created")
    correctMessage.isInstanceOf[Message] should be (true)
    And("There should be 9 recipients")
    correctMessage.getAllRecipients.length should be (9)
    And("The sender should be " + sender)
    correctMessage.getReplyTo.length should be(1)
    correctMessage.getReplyTo.toList.head.toString.equalsIgnoreCase(sender) should be (true)
    correctMessage.getFrom.toList.head.toString.equalsIgnoreCase(sender) should be (true)
    And("The Subject should be " + subject)
    correctMessage.getSubject should be(subject)
    And("The content should be " + content)
    correctMessage.getContent should be (content)

    Given("A call to createMessage with the subject " + subject)
    And("The content is " + content)
    And("The message is from " + sender)
    And("A To list of " + invalidToList.mkString(","))
    And("A CC list of " + ccList.mkString(","))
    And("A BCC list of " + bccList.mkString(","))

    When("The method is called")
    Then("An exception should be raised")
    an[AddressException] should be thrownBy {
      session.composeMessage().
        withSubject(subject).
        withTextBody(content).
        withRecipients(invalidToList).
        withCCRecipients(ccList).
        withBCCRecipients(bccList).withSender(sender)
    }

    Given("A call to createMessage with the subject " + subject)
    And("The content is " + content)
    And("No sender is set")
    And("A To list of " + toList.mkString(","))
    And("A CC list of " + ccList.mkString(","))
    And("A BCC list of " + bccList.mkString(","))
    When("The method is called")
    Then("An exception should be raised")

    an[AddressException] should be thrownBy {
      session.composeMessage().
        withSubject(subject).
        withTextBody(content).
        withRecipients(invalidToList).
        withCCRecipients(ccList).
        withBCCRecipients(bccList).withSender("")
    }
  }

  "MessageHelper" should "Send an email" in {

    val props = new Properties()
    val session = Session.getInstance(props)
    val subject = "TestSubject"
    val content = "MessageContext"
    val sender = "c.kearney@kainos.com"
    val toList = List("to@kainos.com", "to@kainos.com", "to@kainos.com")
    val ccList = List("cc@kainos.com", "cc@kainos.com", "cc@kainos.com")
    val bccList = List("bcc@kainos.com", "bcc@kainos.com", "bcc@kainos.com")

    Given("A call to createMessage.send with the subject " + subject)
    And("The content is " + content)
    And("The message is from " + sender)
    And("A To list of " + toList.mkString(","))
    And("A CC list of " + ccList.mkString(","))
    And("A BCC list of " + bccList.mkString(","))

    When("A connection can be opened")
    session.composeMessage().
      withSubject(subject).
      withTextBody(content).
      withRecipients(toList).
      withCCRecipients(ccList).
      withBCCRecipients(bccList).withSender(sender).send()

    Then("The message should be sent")
    val toStore = session.getStore("mock_pop3")
    toStore.connect("to@kainos.com", null)
    val toInbox = toStore.getFolder("INBOX")
    toInbox.open(Folder.READ_ONLY)

    toInbox.getMessageCount should be (3)
    toInbox.getMessage(1) should not be null

    Then("A Message should be created")
    toInbox.getMessage(1).isInstanceOf[Message] should be (true)
    And("There should be 9 recipients")
    toInbox.getMessage(1).getAllRecipients.length should be (9)
    And("The sender should be " + sender)
    toInbox.getMessage(1).getReplyTo.length should be(1)
    toInbox.getMessage(1).getReplyTo.toList.head.toString.equalsIgnoreCase(sender) should be (true)
    toInbox.getMessage(1).getFrom.toList.head.toString.equalsIgnoreCase(sender) should be (true)
    And("The Subject should be " + subject)
    toInbox.getMessage(1).getSubject should be(subject)
    And("The content should be " + content)
    toInbox.getMessage(1).getContent should be (content)

    toInbox.close(false)
    toStore.close()
  }

  "MessageHelper" should "Check connection details" in {

    Given("A mail session is being established")
    When("TLS is required and no port is supplied")
    Then("An exception should be raised")
    an[ConnectionDetailsNotSetException] should be thrownBy {
      MessageHelper.createMailSession("", requiresAuth = true, enableTLS = false)
    }

    Given("A mail session is being established")
    When("Authentication is required and no username is supplied")
    Then("An exception should be raised")
    an[ConnectionDetailsNotSetException] should be thrownBy {
      MessageHelper.createMailSession("", requiresAuth = false, enableTLS = true, password = "Yep")
    }

    Given("A mail session is being established")
    When("Authentication is required and no password is supplied")
    Then("An exception should be raised")
    an[ConnectionDetailsNotSetException] should be thrownBy {
      MessageHelper.createMailSession("", requiresAuth = false, enableTLS = true, username = "Yep")
    }
  }
}
