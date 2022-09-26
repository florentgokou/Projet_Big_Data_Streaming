import SparkBigData._
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.util._
import org.elasticsearch.spark.sql._
import org.apache.commons.httpclient.HttpConnectionManager
import org.apache.commons.httpclient._

object Spark_ElasticSearch {

  def main(args: Array[String]): Unit = {

    val ss = SparkBigData.Session_Spark(true)
    ss.sparkContext.setLogLevel("OFF")

    println("Contenu de :  \\Ressources\\DataFrame\\orders_csv.csv")
    val df_orders = ss.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferShema", "true")
      .load("D:\\Dossier principal\\Bethel_Info_Service\\Formation Juvénal Data Engenieur\\Ressources\\DataFrame\\orders_csv.csv")

    println("Affichage des données orders_csv.csv")
    //df_orders.show(100)
    println("// écriture des données du DataFrame dans un index d'ElasticSearch")
    df_orders.write   // écriture des données du DataFrame dans un index d'ElasticSearch
      .format("org.elasticsearch.spark.sql")
      .mode(SaveMode.Append)
      .option("es.port","9200")
      .option("es.nodes","localhost")
      .option("es.net.http.auth.user","elastic")
      .option("es.net.http.auth.pass","dWvf5nkkfiva5Wqs18VB")
      .save("index_jvc/doc")

    // Autre methode
    val session_s = SparkSession.builder()
      .appName("Nom Application Spark")
      .config("spark.serializer", "org.apache.spark.serializer.kryoSerializer")
      .config("spark.sql.crossJoin.enabled", "true")
      .config("es.port","9200")
      .config("es.nodes","localhost")
      .config("es.net.http.auth.user","elastic")
      .config("es.net.http.auth.pass", "dWvf5nkkfiva5Wqs18VB")
      .enableHiveSupport()
    df_orders.saveToEs("index_jvc/doc")

  }
}
