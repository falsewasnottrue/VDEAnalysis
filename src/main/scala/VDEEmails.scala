import org.json4s.scalap.Success

import java.util.Base64
import scala.io.Source
import scala.util.Try

object VDEEmails extends App {

  /**
  val input = "70f58720-4de8-11e7-8fce-8742eafd0e9b,2017-06-10 14:23:58.098 UTC,2017-06-10 14:23:58.098 UTC,UfyPZXp5ImIgXHk2g6lQdUJFLiUkmDy1PPSHaSBra/w="
  val res = input.substring(input.lastIndexOf("UTC,") +4)
  println(res)

  val digest = Base64.getDecoder.decode(res)
  println(digest)

  val hexString = bytesToHex(digest)
  println(hexString)
  println(hexString.size)
   */

  def bytesToHex(digest: Array[Byte]): String = {
    val hexString = new StringBuilder()
    for (element <- digest) {
      hexString.append(element.formatted("%02x"))
    }
    hexString.toString()
  }

  // load all emails for vde
  val path = "/Users/rkuschel/tmp/vermietet-de_emails_20210720.csv"

  val bufferedSource = Source.fromFile(path)

  /**
  var successful = 0
  for (line <- bufferedSource.getLines()) {
    try {
      val base64Email = line.substring(line.lastIndexOf("UTC,") + 4)
      Base64.getDecoder.decode(base64Email)
      successful = successful + 1
    } catch {
      case e: IllegalArgumentException => println(e)
    }
  }

  println("size: " + bufferedSource.size)
  println("successful: " + successful)
  */

  val emails: List[String] = bufferedSource.getLines().toList.map { line =>
    Try {
      val base64Email = line.substring(line.lastIndexOf("UTC,") + 4)
      val digest = Base64.getDecoder.decode(base64Email)
      digest
    }
  } collect {
    case scala.util.Success(digest) =>
      val hexString = bytesToHex(digest)
      hexString
  }

  println(emails.size) // 137056
  println(emails.count(_.length != 64))
}
