package datalake.Sales
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import datalake.schema.salesSchema._
import java.time.LocalDateTime
import datalake.config.SparkConfig
import org.apache.log4j.{Level, LogManager, Logger}


object SalesReport extends SparkConfig{
  override val LOG = LogManager.getLogger(this.getClass.getName)
   LOG.setLevel(Level.INFO)


  def main(args: Array[String]): Unit = {
   // val spark = SparkSession.builder.master("local[*]").appName("SalesReport").getOrCreate()
    val spark = buildSession("SalesReport")
    import spark.implicits._

    LOG.info("Application Started"+spark.sparkContext.appName)
    val inputPath = if(!args.isEmpty) args(0) else "./src/test/resources/Sales.csv"
    //val outputPath = if(args.length > 1) args(1) else "./target/test-" + LocalDateTime.now()
    val outputPath = if(args.length > 1) args(1) else "./target/test-" + "sales"


    run(spark, inputPath,outputPath)

    LOG.info("Application Done: " + spark.sparkContext.appName)
    spark.stop()
  }

  def run(spark:SparkSession,inputPath:String,outputPath:String):Unit = {
     LOG.info("Reading text file from: " + inputPath)
     LOG.info("Writing csv to directory: " + outputPath)

    import spark.implicits._
    val firstDataSet:Dataset[String]  = spark.read.option("dateFormat", "yyyyMMdd").textFile(inputPath)

    val salesMapData:Dataset[Sales] =  readDataSet(spark,firstDataSet)

    val groupedSalesData:Dataset[Row] = groupByData(spark,salesMapData)

    groupedSalesData.show()

    //groupedSalesData.write.parquet(outputPath)
    groupedSalesData.write.csv(outputPath)

  }


  def readDataSet(spark:SparkSession,dataSet: Dataset[String]):Dataset[Sales] = {
    import spark.implicits._
    val salesData = dataSet.filter(!_.contains("Region")).map{ line =>
      val p = line.split(",")
      Sales(p(0),p(1),p(2),p(3),p(4),p(5),p(6),(p(7)),convertStringtoDouble(p(8)),convertStringtoDouble(p(9)),convertStringtoDouble(p(10))
        ,convertStringtoDouble(p(11)),convertStringtoDouble(p(12)),convertStringtoDouble(p(13)))
    }
    salesData
  }

  def convertStringtoDouble(input:String):Double = {
    input.toDouble
  }

  def groupByData(spark:SparkSession,salesDataset: Dataset[Sales]):Dataset[Row] ={
    import spark.implicits._
    val onLineSales = salesDataset.where('salesChannel === "Online").groupBy('country,'itemType,'salesChannel)
                    .agg(sum('unitsSold),sum('totalRev))
                    .withColumnRenamed("sum(unitsSold)","sumUnitsSold")
                    .withColumnRenamed("sum(totalRev)","sumTotalRev")

    val offLineSale:Dataset[Row]  = salesDataset.where('salesChannel === "Offline")
      .groupBy('country,'itemType,'salesChannel).agg(sum('unitsSold),sum('totalRev))
      .withColumnRenamed("sum(unitsSold)","sumUnitsSold")
      .withColumnRenamed("sum(totalRev)","sumTotalRev")

      // offLineSale.show()

    onLineSales.createOrReplaceTempView("onLinesSales")
    offLineSale.createOrReplaceTempView("offLineSales")

    val transposeSales:Dataset[Row] = spark.sql(
      """select country,itemType,onlineUnits,onlineRev,offlineUnits,offlineRev from
         (select country,itemType,sumUnitsSold as onlineUnits,round(sumTotalRev,2) as onlineRev from onLinesSales)
          JOIN
         (select country,itemType,sumUnitsSold as offlineUnits,round(sumTotalRev,2) as offlineRev from offLineSales)
         USING (country,itemType)
      """.stripMargin
    )

    transposeSales.filter('country === "United Kingdom" && 'itemType === "Cereal")
  }








}
