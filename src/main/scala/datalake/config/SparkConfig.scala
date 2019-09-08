package datalake.config
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkConfig {
  val LOG = LogManager.getLogger(classOf[SparkConfig])
  val SPARK_MASTER = "local[*]"
  def buildSession(applicaton: String): SparkSession = {
   // SparkSession.builder.appName(applicaton).config(buildConfig(applicaton)).enableHiveSupport.getOrCreate
    SparkSession.builder.appName(applicaton).config(buildConfig(applicaton)).getOrCreate

  }

  def buildConfig(applicaton: String):SparkConf = {

    val conf = new SparkConf().setAppName(applicaton).setMaster(SPARK_MASTER)
   // conf.set("hive.exec.dynamic.partition", "true")
   // conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    conf
  }

}
