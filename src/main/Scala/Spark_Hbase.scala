import SparkBigData._
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.hbase._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Spark_Hbase {
  def catalog_orders = s"""{
                         |  "table":{"namespace":"default", "name":"table_orders"},
                         |  "rowkey":"key",
                         |  "columns":{
                         |	"order_id":{"cf":"rowkey", "col":"key", "type":"string"},
                         |	"customer_id":{"cf":"orders", "col":"customerid", "type":"string"},
                         |	"campaign_id":{"cf":"orders", "col":"campaignid", "type":"string"},
                         |	"order_date":{"cf":"orders", "col":"orderdate", "type":"string"},
                         |	"city":{"cf":"orders", "col":"city", "type":"string"},
                         |	"state":{"cf":"orders", "col":"state", "type":"string"},
                         | "zip_code":{"cf":"orders", "col":"zipcode", "type":"string"},
                         |	"payment_type":{"cf":"orders", "col":"paymenttype", "type":"string"},
                         |	"totalprice":{"cf":"orders", "col":"totalprice", "type":"string"},
                         |	"numorder_lines":{"cf":"orders", "col":"numorderlines", "type":"string"},
                         |	"num_units":{"cf":"orders", "col":"numunits", "type":"string"}
                         |  }
                         |}""".stripMargin

  def main(array: Array[String]): Unit = {
    val ss = Session_Spark(true)
    ss.sparkContext.setLogLevel("OFF")
    val df_hbase = ss.read
      .options(Map(HBaseTableCatalog.tableCatalog -> catalog_orders))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    println("df_hbase.printSchema()")
    df_hbase.printSchema()
    println("df_hbase.show(truncate = false)")
    df_hbase.show(truncate = false)
    df_hbase.createOrReplaceTempView("Orders")
    println("ss.sql(\"select * from Orders where state='MA'\").show()")
    ss.sql("select * from Orders where state='MA'").show()
  }

}
