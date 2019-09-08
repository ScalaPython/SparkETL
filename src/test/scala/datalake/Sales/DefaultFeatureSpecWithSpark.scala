package datalake.Sales
import datalake.config.SparkConfig
import org.apache.spark.sql.SparkSession
import org.scalatest.{FeatureSpec, GivenWhenThen, Matchers}

class DefaultFeatureSpecWithSpark extends  FeatureSpec with GivenWhenThen with Matchers with SparkConfig {
 val spark = buildSession("Spark Scala Testing")
}



