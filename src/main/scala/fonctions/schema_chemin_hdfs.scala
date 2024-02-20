package fonctions

import org.apache.spark.sql.types._
import java.util.Calendar

object schema_chemin_hdfs {


  val schemaRechargeInDetailDF = StructType(
    Array(
      StructField("msisdn"                ,     StringType  , nullable = true)  ,
      StructField("channel_name"          ,     StringType  , nullable = true)  ,
      StructField("channel_ca_recharge"   ,     StringType  , nullable = true)  ,
      StructField("type_recharge"         ,     StringType  , nullable = true)  ,
      StructField("canal_recharge"        ,     StringType  , nullable = true)  ,
      StructField("montant"               ,     LongType    , nullable = true)  ,
      StructField("segment"               ,     StringType  , nullable = true)  ,
      StructField("formule"               ,     StringType  , nullable = true)  ,
      StructField("marche"                ,     StringType  , nullable = true)  ,
      StructField("region"                ,     StringType  , nullable = true)  ,
      StructField("departement"           ,     StringType  , nullable = true)  ,
      StructField("commune_arrondissement",     StringType  , nullable = true)  ,
      StructField("zone_drv"              ,     StringType  , nullable = true)  ,
      StructField("zone_drvnew"           ,     StringType  , nullable = true)  ,
      StructField("zone_dvri"             ,     StringType  , nullable = true)  ,
      StructField("zone_dvrinew"          ,     StringType  , nullable = true)  ,
      StructField("year"                  ,     StringType  , nullable = true)  ,
      StructField("month"                 ,     StringType  , nullable = true)  ,
      StructField("day"                   ,     StringType  , nullable = true)
    )
  )



  val schemaRechargeDetaillee = StructType(
    Array(
      StructField("date_recharge"       ,     TimestampType , nullable = true)  ,
      StructField("heure_recharge"      ,     StringType    , nullable = true)  ,
      StructField("canal_recharge"      ,     StringType    , nullable = true)  ,
      StructField("rech_carte_verte"    ,     StringType    , nullable = true)  ,
      StructField("montant_recharge"    ,     DoubleType    , nullable = true)  ,
      StructField("type_recharge"       ,     StringType    , nullable = true)  ,
      StructField("type_recharge_desc"  ,     StringType    , nullable = true)  ,
      StructField("msisdn"              ,     StringType    , nullable = true)  ,
      StructField("formule"             ,     StringType    , nullable = true)  ,
      StructField("year"                ,     StringType    , nullable = true)  ,
      StructField("month"               ,     StringType    , nullable = true)  ,
      StructField("day"                 ,     StringType    , nullable = true)

    )
  )

  def annee(): Int = {

    // Obtenir la date du mois précédent
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.MONTH, -1)
    val year = calendar.get(Calendar.YEAR)
    year
  }

  def moisPrecedent() : String = {
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.MONTH, -1)
    val month = calendar.get(Calendar.MONTH) + 1
    val formattedMonth = if (month < 10) s"0$month" else month.toString
    formattedMonth
  }

  val chemin_in_detail = "/dlk/osn/refined/Recharge/recharge_in_detail/year="+annee()+"/month="+moisPrecedent()

  val chemin_detaillee = "/dlk/osn/refined/Recharge/recharge_detaillee/year="+annee()+"/month="+moisPrecedent()

  val table_read_in_detail = "refined_recharge.recharge_in_detail"
  val table_read_detaillee = "refined_recharge.recharge_detaillee"


  val chemin_write_in_detail = "/warehouse/tablespace/external/hive/dfc_temp.db/reconciliation_recharge_in_zebra"
  val chemin_write_detaillee = "/warehouse/tablespace/external/hive/dfc_temp.db/recharge_in_zebra_agr"

  val table_write_in_detail = "dfc_temp.reconciliation_recharge_in_zebra"
  val table_write_detaillee = "dfc_temp.recharge_in_zebra_agr"

}
