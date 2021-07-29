import org.apache.spark.sql.{Row, SparkSession}

import java.util.Base64
import scala.io.Source
import scala.util.Try

object VDEAnalysis extends App {

  def bytesToHex(digest: Array[Byte]): String = {
    val hexString = new StringBuilder()
    for (element <- digest) {
      hexString.append(element.formatted("%02x"))
    }
    hexString.toString()
  }

  // Load VDE emails
  val vdePath = "/Users/rkuschel/tmp/vermietet-de_emails_20210720.csv"
  val bufferedSource = Source.fromFile(vdePath)

  case class VdeEmail(customerId: String, hash: String)

  val vdeEmails: List[VdeEmail] = bufferedSource.getLines().toList.map { line =>
    val customerId = line.substring(0, line.indexOf(","))
    Try {
      val base64Email = line.substring(line.lastIndexOf("UTC,") + 4)
      val digest = Base64.getDecoder.decode(base64Email)
      (customerId, digest)
    }
  } collect {
    case scala.util.Success((customerId, digest)) =>
      val hexString = bytesToHex(digest)
      VdeEmail(customerId, hexString)
  }

  println("VDE emails: " + vdeEmails.size) // 137056
  if (vdeEmails.count(_.hash.length != 64) != 0) {
    throw new IllegalArgumentException("Unexpected SHA256 in VDE Emails")
  }

  // Load scout24 emails

  val spark: SparkSession = SparkSession.builder()
    .appName("VDEAnalysis")
    .master("local[1]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()

  // val path = "/Users/rkuschel/tmp/record_4960655756899914296.avro"
  // val path = "/Users/rkuschel/tmp/record_5415379404911843680.avro"
  // val path = "/Users/rkuschel/tmp/record_5682326664629851450.avro"
  // val path = "/Users/rkuschel/tmp/record_644555388435776715.avro"
  val path = "/Users/rkuschel/tmp/record_8060804577261386866.avro"

  val df = spark.read
    .format("com.databricks.spark.avro")
    .load(path)

  // df.printSchema()
  // root
  // |-- ssoid: long (nullable = true)
  // |-- email_hash: string (nullable = true)
  // |-- __version: long (nullable = true)

  // val size = df.count()
  // println(size)
  // 6027559

  // get first email hash
  // val rows = df.take(10)
  // rows.foreach(row => println(row))
  // [82515573,8d8d0bb85513fa0e8dea762ad798597dae3176dc713f495106ff7c0e3cd8f5bf,1]
  // [97333723,6875defe62fd11a9f44239450dba04fdb597734062df77bdc40295eed6d1cc0b,1]
  // [96003871,19882c7be95b71d289902013ea7ea4efb354873d59038c11db6191ca54a3c3a0,1]
  // [111316417,493aed10610c5d3cfabf5b7a19248c9a4dd6e4c9ccf31218f1b0ce2fc6ae5fed,1]
  // [79144106,b5fbfdbba64b31587a1ced04915019d43f31431817989cc7ec667a01cf13b984,1]
  // [79144126,66f1791b623183bd53d4d057fe4c72d23f258316eaa144467ba06d32ee44ce69,1]
  // [86667569,ad7883f1749a2899ec968628769680bfd988010f9c112dee7bfa03de1d6a540b,1]
  // [94376943,ed9ce6984e8d581ce801767164c9d16b95c04db96c7cf1eb75e86176cf8ef3e0,1]
  // [106726945,e84da2ad61491d67e50487af83557721d520263f30c777f4758468888da55733,1]
  // [114299575,c50b9ea7d32a839cb61c248d0b8f19d688cbc17bc388a080054666fb4e757606,1]
  // -> no SSO Ids in tat
  // sha256 string representation with 64 characters

  // create a hashmap of all hashed emails
  val start = System.currentTimeMillis()
  println("Start " + start)

  val scoutEmails = new scala.collection.mutable.HashMap[String, Int]()
  df.rdd.foreach {
    case Row(_, email: String, _) => scoutEmails.put(email, scoutEmails.getOrElse(email, 0)+1)
  }

  val end = System.currentTimeMillis()
  println("Took " + (end - start))
  println("Scout24 emails: " + scoutEmails.keySet.size)
  // TODO duplicates?

  val found = new scala.collection.mutable.HashMap[Int, Int]()
  val counted = new scala.collection.mutable.HashMap[Int, List[VdeEmail]]()

  // Check if Scout emails contain VDE emails
  for (email <- vdeEmails) {
    if (scoutEmails.contains(email.hash)) {
      val count = scoutEmails.getOrElse(email.hash, 1)
      found.put(count, found.getOrElse(count, 0) + 1)
      // println("found: " + email + " " + scoutEmails.get(email))

      if (count > 1) {
        val listOfEmails = counted.getOrElse(count, Nil)
        counted.put(count, listOfEmails :+ email)
      }
    }
  }

  for (count <- found.keySet) {
    println(" " + count + " -> " + found.getOrElse(count, 0))
  }

  println("---")
  for (count <- counted.keySet) {
    val listOfEmails = counted.getOrElse(count, Nil)
    println(" " + count + " -> " + listOfEmails.size)
  }

  println("---")
  for (count <- counted.keySet) {
    val listOfEmails = counted.getOrElse(count, Nil)
    listOfEmails.foreach(email =>
      println(count + "," + email.customerId + "," + email.hash)
    )
  }
}
