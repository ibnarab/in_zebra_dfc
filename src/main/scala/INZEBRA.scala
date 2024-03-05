import fonctions.{mail, read_write, schema_chemin_hdfs, utils}
import org.apache.spark.sql.SaveMode
object INZEBRA {



  def main(args: Array[String]): Unit = {

    val debut                         = args(0)
    val fin                           = args(1)
    val partitionmonth                = args(2)




    val rechargeInDetail              = read_write.readRechargeInDetail(schema_chemin_hdfs.table_read_in_detail, debut, fin)
    val rechargeDetaillee             = read_write.readRechargeDetaillee(schema_chemin_hdfs.table_read_detaillee, debut, fin)

    val rechargeInDetailFiltre        = utils.filtre_recharge_in_detail(rechargeInDetail)
    val rechargeDetailleeFiltre       = utils.filtre_recharge_detaillee(rechargeDetaillee)


    val inDetailAddRenameColumns      = utils.add_columns_and_rename_detail_in(rechargeInDetailFiltre)
    val detailleeAddRenameColumns     = utils.add_columns_and_rename_detaillee(rechargeDetailleeFiltre)
    val dfJoin                        = utils.reconciliationJOIN(inDetailAddRenameColumns, detailleeAddRenameColumns)
    val dfInZebra                     = utils.reconciliationInZebra(dfJoin)

    val dfAgg                         = utils.reconciliationAgregee(dfJoin)


    read_write.writeHiveInZebra(dfInZebra, schema_chemin_hdfs.header, schema_chemin_hdfs.table_write_reconciliation)

    read_write.writeHiveAgr(dfAgg, schema_chemin_hdfs.header, schema_chemin_hdfs.table_write_agr)

    dfJoin.write.option("header", true).mode(SaveMode.Overwrite).saveAsTable("dfc_temp.joinTest")



    read_write.writeMail(dfAgg, partitionmonth)
    mail.sendMail(partitionmonth)


    println("debut = "+debut)
    println("fin = "+fin)
    println("PartitionMonth = "+partitionmonth)



  }

}
