package fonctions

import javax.activation.DataHandler
import javax.mail.internet.{MimeBodyPart, MimeMultipart}
import javax.mail.util.ByteArrayDataSource
import org.apache.commons.mail.MultiPartEmail
import org.apache.hadoop.fs.{FileSystem, Path}


object mail {


  def sendMail(partDay: String) = {
    val multipart = new MimeMultipart()

    val messageBodyPart = new MimeBodyPart()

    val fs = FileSystem.get(constants.spark.sparkContext.hadoopConfiguration)
    val stream = fs.open(new Path(s"hdfs://bigdata/dlk/osn/refined/reconciliation_recharge_in_zebra/reconciliation_recharge_in_zebra.$partDay.xlsx"))
    messageBodyPart.setDataHandler(new DataHandler(new ByteArrayDataSource(stream,"application/vnd.ms-excel")))
    messageBodyPart.setFileName(s"reconciliation_recharge_in_zebra.$partDay.xlsx")


    multipart.addBodyPart(messageBodyPart)
    val objet = s"RECONCILIATION RECHARGE IN ZEBRA  $partDay"
    println(objet)
    val  corps = s"Bonjour , \nCi-joint les reportings :\n* Reconciliation Recharge par jour et par type de recharge.\n* reconciliation_recharge_in_zebra $partDay \n\nCordialement, \nL'équipe DBM"
    println(corps)

    val RECEIVER = "mouhamedibnarab.diop@orange-sonatel.com,ndeyerokhaya.dia@orange-sonatel.com,mohamed.diene@orange-sonatel.com,ababacar.diouf@orange-sonatel.com,aminatamacky.tall@orange-sonatel.com,cheikhmarieteuw.diop@orange-sonatel.com,team_fra@orange-sonatel.com"
    //val RECEIVER = "mouhamedibnarab.diop@orange-sonatel.com"
    println(RECEIVER)
    val SENDER = "reconciliation_recharge_in_zebrag@orange-sonatel.com"
    println(SENDER)
    val email = new MultiPartEmail()
    email.setHostName("10.100.56.56")
    email.setFrom(SENDER)
    email.setContent(multipart)
    email.addPart(multipart)

    email.setSubject(objet)
    email.setMsg(corps)

    email.addTo(RECEIVER.split(",").toSeq: _*)

    email.send()

    val status = "Message envoyer avec succes a " + RECEIVER
    println(status)
  }

}
