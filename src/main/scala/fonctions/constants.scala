package fonctions

import org.apache.spark.sql.SparkSession

object constants {

  val spark = SparkSession.builder
    .appName("reconciliation")
    .config("spark.hadoop.fs.defaultFS", "hdfs://bigdata")
    .config("spark.master", "yarn")
    .config("spark.submit.deployMode", "cluster")
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .enableHiveSupport()
    .getOrCreate()

}
