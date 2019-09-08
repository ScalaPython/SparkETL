package datalake.schema

import org.apache.spark.sql.Row
import org.apache.spark.sql.RowFactory
import org.apache.log4j.{Level, LogManager, Logger}

object salesSchema {
  val LOG = LogManager.getRootLogger

  case class Sales(region:String,country:String,itemType:String,salesChannel:String,
                   orderPri:String,orderDate:String,orderId:String,shipDate:String,
                   unitsSold:Double,unitPrice:Double,unitCost:Double,totalRev:Double,
                   totalCost:Double,totalProfit:Double
                  ) /*{
    def buildRow:Row = {
      RowFactory.create(region,country,itemType,salesChannel,
        orderPri,orderDate,orderId,shipDate,
        unitsSold,unitPrice,unitCost,totalRev,
        totalCost,totalProfit)
  } */




  case class Country(cname:String,uniqueID:Long)


}
