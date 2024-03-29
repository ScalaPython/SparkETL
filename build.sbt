name := "SparkETL"
version := "0.1"
scalaVersion := "2.11.0"
val sparkVersion = "2.4.3"


libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "com.typesafe" % "config" % "1.3.2",
      "org.scalatest" %% "scalatest" % "3.0.5" % "test"

)
    
