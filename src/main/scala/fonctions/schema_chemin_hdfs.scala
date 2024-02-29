package fonctions

import java.util.Calendar

object schema_chemin_hdfs {

  def anneeMoisPrecedent(): String = {
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.MONTH, -1)
    val year = calendar.get(Calendar.YEAR)
    val month = calendar.get(Calendar.MONTH) + 1
    val formattedMonth = if (month < 10) s"0$month" else month.toString
    s"$year-$formattedMonth"
  }

  val table_read_in_detail = "refined_recharge.recharge_in_detail"
  val table_read_detaillee = "refined_recharge.recharge_detaillee"


  val chemin_write_in_detail = "/warehouse/tablespace/external/hive/dfc_temp.db/reconciliation_recharge_in_zebra"
  val chemin_write_detaillee = "/warehouse/tablespace/external/hive/dfc_temp.db/recharge_in_zebra_agr"

  val table_write_reconciliation = "dfc_temp.reconciliation_recharge_in_zebra"
  val table_write_agr = "dfc_temp.recharge_in_zebra_agr"

  val header = true



}
