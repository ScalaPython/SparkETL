package datalake.Sales

import java.nio.file.{Files, StandardOpenOption}
import java.util.Date

import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter._
import org.apache.spark.sql.{Dataset, Row, SparkSession}


class SalesReportTest extends DefaultFeatureSpecWithSpark {

  feature("Sales Report Validation") {
    scenario("Acceptance test for basic use") {
      Given("A simple input file, a Spark context, and a known output FIle in Target")

     // val rootDirectory = Files.createTempDirectory(this.getClass.getName)
     // val inputFile = Files.createFile(rootDirectory.resolve("input.txt"))
      // val inputFile = Files.createFile(rootDirectory.resolve("input.txt"))
      val inputFile =  "./src/test/resources/Sales.csv"
      // val outputDirectory = rootDirectory.resolve("output")
      val outputPath = "./target/test-" + "sales"
      import scala.collection.JavaConverters._
       SalesReport.run(spark,
         inputFile,
         outputPath)
      val expectedLines:Array[Row] = Array(Row("United Kingdom","Cereal",22180.0,4562426.0,15558.0,3200280.6))
      println(expectedLines.mkString(","))
      val firstDataSet:Dataset[Row] = spark.read.csv(outputPath)
      val allLines :Array[Row]  = firstDataSet.collect()
      println(allLines.mkString(","))

     // Files.write(inputFile, lines.asJava, StandardOpenOption.CREATE)

      When("I trigger the application")

    //  SalesReport.run(spark,
     //   inputFile.toUri.toString,
      //  outputDirectory.toUri.toString)

     Then("It outputs files containing the expected data")



      allLines.mkString(",") should contain theSameElementsAs expectedLines.mkString(",")
    }


  }

  private def makeInputAndOutputDirectories(folderNameSuffix: String): (String, String) = {
    val rootDirectory =
      Files.createTempDirectory(this.getClass.getName + folderNameSuffix)
    val ingestDir = rootDirectory.resolve("ingest")
    val transformDir = rootDirectory.resolve("transform")
    (ingestDir.toUri.toString, transformDir.toUri.toString)
  }

}
