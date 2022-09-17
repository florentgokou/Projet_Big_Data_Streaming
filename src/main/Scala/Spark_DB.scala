import SparkBigData._
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.util._

object Spark_DB {
  def main(arg: Array[String]): Unit = {
    Session_Spark(Env = true).sparkContext.setLogLevel("OFF")
    val ss = Session_Spark(true)
    val props_mysql = new Properties()
    props_mysql.put("user","consultant")
    props_mysql.put("password", "pwd#86")

    val props_sqlserver = new Properties()
    props_sqlserver.put("user", "consultant")
    props_sqlserver.put("password", "pwd#86")

    val props_prostgre = new Properties()
    props_prostgre.put("user", "consultant")
    props_prostgre.put("password", "pwd#86")
/*
    // Création du DataFrame qui contiendra les données venant de Mysql
    println("Affichage des données du DataFrame qui contiendra les données venant de Mysql")
    val df_mysql = ss.read.jdbc("jdbc:mysql://127.0.0.1:3306/jea_db","jea_db.orders",props_mysql)
    df_mysql.show(15)
    //df_mysql.printSchema()
    */

/*
    //
    // Autre methode : Création du DataFrame qui contiendra les données venant de Mysql
    // Cette requête ne marchera pas car dans Mysql, la requête avec l'option "Query" n'est pas autorisée
    val df_mysql2 = ss.read
      .format("jdbc")
      .option("url","jdbc:mysql://127.0.0.1:3306/jea_db")
      .option("user","consultant")
      .option("password", "pwd#86")
      .option("dbtable", "(select state, city, sum( round(numunits * totalprice )) as Commande_Totales from jea_db.orders group by state, city) table_summary")
        .load()
      //.option("query","select state, city, sum( round(numunits * totalprice )) as Commande_Totales from jea_db.orders group by state, city")
    //df_mysql2.show()
*/

    /*
    println("//Spark & PostgreSQL : Interrogez les bases PostgreSQL")
    val df_postgre = ss.read.jdbc("jdbc:postgresql://127.0.0.1:5432/jea_db","orders",props_prostgre)
    println("Affichage du resultat - PostgreSQL")
    df_postgre.show(15)
    */


    /*
    println("Autre methode : Création du DataFrame qui contiendra les données venant de PostgreSQL")
    // Cette requête ne marchera pas car dans PostgreSQL, la requête avec l'option "Query" n'est pas autorisée
    val df_postgre2 = ss.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://127.0.0.1:5432/jea_db")
      .option("user", "consultant")
      .option("password", "pwd#86")
      .option("dbtable", "(select state, city, round(sum( numunits * totalprice),2) as Commande_Totales from orders group by state, city) table_summary")
      .load()
    //.option("query","select state, city, sum( round(numunits * totalprice )) as Commande_Totales from jea_db.orders group by state, city")
    println("Affichage du resultat - Autre methode avec PostgreSQL")
    df_postgre2.show()
    */
/*
    println("Spark & SQL Server : Interrogez les bases SQL Server")
    val df_sqlserver = ss.read.jdbc("jdbc:sqlserver://DESKTOP-93Q8BKB\\SPARKSQLSERVER:1433;databaseName=jea_db","orders",props_prostgre)
    println("Afficher les données des la base SQL Server")
    df_sqlserver.show(15)
*/

    println("Autre methode : Création du DataFrame qui contiendra les données venant de sqlserver")
    // Cette requête ne marchera pas car dans sqlserver, la requête avec l'option "Query" n'est pas autorisée
    val df_sqlserver2 = ss.read
      .format("jdbc")
      .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("url", "jdbc:sqlserver://DESKTOP-93Q8BKB\\SPARKSQLSERVER:1433;databaseName=jea_db;integratedSecurity=true")
      .option("dbtable", "orders")
      .load()
    println("Affichage du resultat - Autre methode avec sqlserver2")
    df_sqlserver2.show(20)
  }

}
