package fonctions

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import constants.spark
import org.apache.spark.sql.functions.{lit, date_format, year, month, size, col, _}



object utils {


  def filtre_recharge_in_detail(df: DataFrame): DataFrame = {

    df.where(
      (col("canal_recharge") === "Ca Credit OM" && col("channel_name")
        .isin("Rech_OM_O&M", "Webservices Recharge(Orange Money)", "Rech_OM_Distri"))
        ||
        (col("canal_recharge") === "CA Wave")
        ||
        (col("canal_recharge") === "Ca IAH")
        ||
        (col("canal_recharge") === "Ca Seddo")
        ||
        (col("canal_recharge") === "Ca Cartes")

    )
  }


  def filtre_recharge_detaillee(df: DataFrame): DataFrame = {

    df.where(
      (col("type_recharge") === "M")
        ||
        (col("type_recharge") === "O")
        ||
        (col("type_recharge") === "D")
        ||
        (col("type_recharge") === "W")
        ||
        (col("type_recharge") === "V")
        ||
        (col("type_recharge") === "A")
        ||
        (col("type_recharge") === "E")
        ||
        (col("type_recharge") === "C")
    )
  }


  def add_columns_and_rename_detail_in(df: DataFrame): DataFrame = {

    val df1 = df
      //.withColumnRenamed("day"             ,   "date")
      .withColumn("year_in", substring(col("day"), 1, 4))
      .withColumn("month_in", substring(col("day"), 5, 2))
      .withColumnRenamed("day", "day_in")
      .withColumnRenamed("msisdn", "numero_in")
      .withColumnRenamed("formule", "formule_in")
      .withColumn("type_recharge_in",
        when(col("canal_recharge") === "Ca Credit OM" && col("channel_name") === "Rech_OM_O&M", lit("Recharge Orange Money O&M"))
          .when(col("canal_recharge") === "Ca Credit OM" && col("channel_name") === "Webservices Recharge(Orange Money)", lit("Recharge Orange Money S2S"))
          .when(col("canal_recharge") === "Ca Credit OM" && col("channel_name") === "Rech_OM_Distri", lit("Recharge Orange Money Distri"))
          .when(col("canal_recharge") === "CA Wave", lit("Recharge Wave"))
          .when(col("canal_recharge") === "Ca IAH", lit("Recharge IAH"))
          .when(col("canal_recharge") === "Ca Seddo" && col("channel_name") === "Corporate Recharge", lit("Seddo Corporate"))
          .when(col("canal_recharge") === "Ca Seddo" && col("channel_name").isin("MobileSelfService", "IT Subscription", "RMS Charging", "Webservices Recharge(C2S)"), lit("Recharge C2S"))
          .when(col("canal_recharge") === "Ca Cartes", lit("Recharge Carte"))
          .otherwise(lit(null))
      )
      .withColumn("montant_in", col("montant").cast("double"))
      .withColumn("date_recharge_in", col("datetime_event").cast("timestamp"))
      .drop(
        "channel_name",
        "channel_ca_recharge",
        "canal_recharge",
        "segment",
        "marche",
        "region",
        "departement",
        "commune_arrondissement",
        "zone_drv",
        "zone_drvnew",
        "zone_dvri",
        "zone_dvrinew",
        "datetime_event"
      )
      .select(
        "date_recharge_in",
        "numero_in",
        "type_recharge_in",
        "formule_in",
        "montant_in",
        "year_in",
        "month_in",
        "day_in"
      )

    df1
      .withColumn("list_in", collect_list("montant_in")
        .over(Window.partitionBy("date_recharge_in", "numero_in", "type_recharge_in")))
  }


  def add_columns_and_rename_detaillee(df: DataFrame): DataFrame = {

    val df1 = df
      //.withColumnRenamed("day"                                , "date")
      .withColumn("year_zebra", substring(col("day"), 1, 4))
      .withColumn("month_zebra", substring(col("day"), 5, 2))
      .withColumnRenamed("day", "day_zebra")
      .withColumnRenamed("msisdn", "numero_zebra")
      .withColumnRenamed("date_recharge", "date_recharge_zebra")
      .withColumnRenamed("montant_recharge", "montant_zebra")
      .withColumn("type_recharge_zebra",
        when(col("type_recharge") === "M", lit("Recharge Orange Money O&M"))
          .when(col("type_recharge") === "O", lit("Recharge Orange Money S2S"))
          .when(col("type_recharge") === "D", lit("Recharge Orange Money Distri"))
          .when(col("type_recharge") === "W", lit("Recharge Wave"))
          .when(col("type_recharge") === "V", lit("Recharge IAH"))
          .when(col("type_recharge") === "A", lit("Seddo Corporate"))
          .when(col("type_recharge") === "E", lit("Recharge C2S"))
          .when(col("type_recharge") === "C", lit("Recharge Carte"))
          .otherwise(lit(null))
      )
      .withColumn("formule_zebra",
        regexp_replace(
          col("formule"),
          "^(HYBRIDE-|POSTPAID-|PREPAID-)",
          ""
        )
      )
      .drop(
        "heure_recharge",
        "canal_recharge",
        "rech_carte_verte",
        "type_recharge_desc"
      )
      .select(
        "date_recharge_zebra",
        "numero_zebra",
        "type_recharge_zebra",
        "formule_zebra",
        "montant_zebra",
        "year_zebra",
        "month_zebra",
        "day_zebra"
      )

    df1.withColumn("list_zebra", collect_list("montant_zebra")
      .over(Window.partitionBy("date_recharge_zebra", "numero_zebra", "type_recharge_zebra")))

  }


  def reconciliationJOIN(df_in: DataFrame, df_zebra: DataFrame): DataFrame = {

    val marginInSeconds = 2

    val df1 = df_in.join(df_zebra,
      (
        df_in("montant_in") =!= df_zebra("montant_zebra") &&
          df_in("date_recharge_in") === df_zebra("date_recharge_zebra") &&
          df_in("numero_in") === df_zebra("numero_zebra") &&
          df_in("type_recharge_in") === df_zebra("type_recharge_zebra")
        )
        ||
        (
          df_in("montant_in") === df_zebra("montant_zebra") &&
            df_in("numero_in") === df_zebra("numero_zebra") &&
            df_in("type_recharge_in") === df_zebra("type_recharge_zebra") &&
            abs(df_in("date_recharge_in").cast("long") - df_zebra("date_recharge_zebra").cast("long")) <= marginInSeconds
          )

      ,
      "full")


    val df2 = df1
      .withColumn("date_recharge",
        when(abs(col("date_recharge_in").cast("long") - col("date_recharge_zebra").cast("long")) <= marginInSeconds, col("date_recharge_in"))
          //when(col("date_recharge_in") === col("date_recharge_zebra"), col("date_recharge_in"))
          .otherwise(coalesce(col("date_recharge_in"), col("date_recharge_zebra"))))
      .withColumn("numero",
        when(col("numero_in") === col("numero_zebra"), col("numero_in"))
          .otherwise(coalesce(col("numero_in"), col("numero_zebra"))))
      .withColumn("type_recharge",
        when(col("type_recharge_in") === col("type_recharge_zebra"), col("type_recharge_in"))
          .otherwise(coalesce(col("type_recharge_in"), col("type_recharge_zebra"))))
      .withColumn("formule",
        when(col("formule_in") === col("formule_zebra"), col("formule_in"))
          .otherwise(coalesce(col("formule_in"), col("formule_zebra"))))
      .withColumn("year",
        when(col("year_in") === col("year_zebra"), col("year_in"))
          .otherwise(coalesce(col("year_in"), col("year_zebra"))))
      .withColumn("month",
        when(col("month_in") === col("month_zebra"), col("month_in"))
          .otherwise(coalesce(col("month_in"), col("month_zebra"))))
      .withColumn("day",
        when(col("day_in") === col("day_zebra"), col("day_in"))
          .otherwise(coalesce(col("day_in"), col("day_zebra"))))
      .withColumn("ecart",
        when(col("date_recharge_in") === col("date_recharge_zebra")
          &&
          col("numero_in") === col("numero_zebra")
          &&
          col("type_recharge_in") === col("type_recharge_zebra")
          &&
          col("montant_in") =!= col("montant_zebra")
          &&
          size(array_except(array(col("montant_zebra")), col("list_in"))) =!= 0
          &&
          size(array_except(array(col("montant_in")), col("list_zebra"))) =!= 0

          ,
          lit("mismatch"))
          .when(col("date_recharge_in").isNull
            &&
            col("numero_in").isNull
            &&
            col("type_recharge_in").isNull
            ,
            lit("missing_in"))
          .when(col("date_recharge_zebra").isNull
            &&
            col("numero_zebra").isNull
            &&
            col("type_recharge_zebra").isNull
            ,
            lit("missing_zebra"))

          .when(
            (abs(col("date_recharge_in").cast("long") - col("date_recharge_zebra").cast("long")) <= marginInSeconds)
            &&
            col("numero_in") === col("numero_zebra")
            &&
            col("type_recharge_in") === col("type_recharge_zebra")
            &&
            col("montant_in") === col("montant_zebra")
            ,
            lit("match")

          )
          .otherwise(null)
      )
      //.withColumn("year",   year(col("input")))
      .drop("date_recharge_in", "date_recharge_zebra", "numero_in",
      "numero_zebra", "type_recharge_in", "type_recharge_zebra", "formule_in", "formule_zebra", "year_in", "year_zebra",
      "month_in", "month_zebra", "day_in", "day_zebra", "list_in", "list_zebra")
      .select("date_recharge", "numero", "type_recharge", "formule", "montant_in", "montant_zebra", "ecart")


    df2
      .withColumn("year", year(col("date_recharge")).cast("String"))
      .withColumn("month", format_string("%02d", month(col("date_recharge"))))
      .withColumn("day", date_format(col("date_recharge"), "yyyyMMdd").cast("String"))


  }

  def reconciliationInZebra(dataFrame: DataFrame): DataFrame = {
    dataFrame.where(col("ecart").isin("missing_in", "missing_zebra", "mismatch"))
  }



  def reconciliationAgregee(dataFrame: DataFrame, date: String): DataFrame = {

          val rows = date.split("\n").toSeq

          // Convertir la séquence en DataFrame avec une seule colonne
          val df = spark.createDataFrame(rows.map(Tuple1.apply)).toDF("day")


        // Liste des types de recharge
        val recharges = List("Recharge Orange Money Distri", "Recharge IAH", "Recharge Orange Money O&M", "Recharge Orange Money S2S", "Recharge Wave", "Recharge C2S", "Recharge Carte", "Seddo Corporate")

        // Créer un DataFrame pour les types de recharge avec un identifiant
        val rechargesDF = spark.createDataFrame(recharges.zipWithIndex).toDF("type_recharge", "recharge_id")

        // Joindre la date avec chaque type de recharge
        val explodedResult = df.crossJoin(rechargesDF)
          .withColumn("year", substring(col("day"), 1, 4))
          .withColumn("month", substring(col("day"), 5, 2))

        val df1 = dataFrame
            .groupBy("type_recharge", "year", "month", "day")
            .agg(
                  count(when(col("ecart") === "match", true)).alias("nombre_match"),
                  count(when(col("ecart") === "mismatch", true)).alias("nombre_mismatch"),
                  count(when(col("ecart") === "missing_in", true)).alias("nombre_missing_in"),
                  count(when(col("ecart") === "missing_zebra", true)).alias("nombre_missing_zebra"),
                  sum(when(col("ecart").isin("match", "mismatch", "missing_zebra"), col("montant_in")).otherwise(0)).alias("in_mnt"),
                  sum(when(col("ecart").isin("match", "mismatch", "missing_in"), col("montant_zebra")).otherwise(0)).alias("ze_mnt"),
                  count(when(col("ecart").isin("match", "mismatch", "missing_zebra"), true)).alias("in_cnt"),
                  count(when(col("ecart").isin("match", "mismatch", "missing_in"), true)).alias("ze_cnt")
            )
            .drop("numero", "formule", "montant_in", "montant_zebra", "ecart")

        val df2 = df1
          .withColumn("ecart_cnt",
            when(col("in_cnt") >= col("ze_cnt"), col("in_cnt") - col("ze_cnt")).otherwise(-(col("ze_cnt") - col("in_cnt"))))
          .withColumn("ecart_mnt",
            when(col("in_mnt") >= col("ze_mnt"), col("in_mnt") - col("ze_mnt")).otherwise(-(col("ze_mnt") - col("in_mnt"))))
          .withColumn("perc_match",
            ((col("nombre_match") / col("in_cnt")) * 100)
          )
          .withColumn("perc_mismatch",
            ((col("nombre_mismatch") / col("in_cnt")) * 100)
          )
          .withColumn("perc_in",
            ((col("nombre_missing_in") / col("in_cnt")) * 100)
          )
          .withColumn("perc_ze",
            ((col("nombre_missing_zebra") / col("in_cnt")) * 100)
          )
          .na.drop(Seq("year", "month", "day"))


        val dfJoin = explodedResult.join(df2, Seq("type_recharge", "day"), "left").select(
          explodedResult("type_recharge")     ,
          df2("in_mnt")                       ,
          df2("ze_mnt")                       ,
          df2("ecart_mnt")                    ,
          df2("in_cnt")                       ,
          df2("ze_cnt")                       ,
          df2("ecart_cnt")                    ,
          df2("nombre_match")                 ,
          df2("perc_match")                   ,
          df2("nombre_mismatch")              ,
          df2("perc_mismatch")                ,
          df2("nombre_missing_in")            ,
          df2("perc_in")                      ,
          df2("nombre_missing_zebra")         ,
          df2("perc_ze")                      ,
          explodedResult("year")              ,
          explodedResult("month")             ,
          explodedResult("day")
        )

        dfJoin
          .na.fill(0, Seq("type_recharge", "in_mnt", "ze_mnt", "ecart_mnt", "in_cnt", "ze_cnt", "ecart_cnt",
          "nombre_match", "perc_match", "nombre_mismatch", "perc_mismatch", "nombre_missing_in", "perc_in", "nombre_missing_zebra", "perc_ze"))
          .orderBy("year", "month", "day")

      }







}
