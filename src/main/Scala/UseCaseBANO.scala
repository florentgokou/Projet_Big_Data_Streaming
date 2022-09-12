import SparkBigData._
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object UseCaseBANO {

  val schema_bano = StructType(Array(
    StructField("id_bano", StringType, false),
    StructField("numero_voie", StringType, false),
    StructField("nom_voie", StringType, false),
    StructField("code_postal", StringType, false),
    StructField("nom_commune", StringType, false),
    StructField("code_source_bano", StringType, false),
    StructField("latitude", StringType, true),
    StructField("longitude", StringType, true)
    )
  )
  val configH = new Configuration()
  val fs = FileSystem.get(configH)
  val chemin_dest = new Path("C:\\Users\\Nathaniel\\Dossier principal\\Bethel_Info_Service\\Formation Juvénal Data Engenieur\\Ressources\\FichiersBANO_DEST\\")


  def main(arg: Array[String] ): Unit = {
    val ss = Session_Spark(true)
    ss.sparkContext.setLogLevel("OFF")

    val df_bano_brut = ss.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .schema(schema_bano)
      .csv("C:\\Users\\Nathaniel\\Dossier principal\\Bethel_Info_Service\\Formation Juvénal Data Engenieur\\Ressources\\FichiersBANO\\full.csv")
    //df_bano_brut.show(10)

    // Transformation
    val df_bano = df_bano_brut
      .withColumn("code_departement", substring(col("code_postal"), pos = 1, len = 2))
      .withColumn("libelle_source", when(col("code_source_bano") === lit("OSM"), lit("OpenStreeMap"))
        .otherwise(when(col("code_source_bano") === lit("OD"), lit("OpenData"))
          .otherwise(when(col("code_source_bano") === lit("O+0"), lit("Open OSM"))
            .otherwise(when(col("code_source_bano") === lit("CAD"), lit("Cadastre"))
              .otherwise(when(col("code_source_bano") === lit("C+O"), lit("Cadastre OSM"))))))
      )
    //df_bano.show(100)

    val df_departement = df_bano.select("code_departement").distinct().filter(col("code_departement").isNotNull)
      .orderBy("code_departement")
    //df_departement.show()

    val liste_departement = df_bano.select(col("code_departement"))
      .distinct()
      .orderBy("code_departement")
      .filter(col("code_departement").isNotNull)
      .collect()
      .map(x => x(0)).toList


    liste_departement.foreach {
      x =>
        df_bano.filter(col("code_departement") === x.toString)
          .coalesce(1)
          .where(col("code_source_bano") === lit("C+O"))
          .write
          .format("com.databricks.spark.csv")
          .option("delimiter", ";")
          .option("header", "true")
          .mode(SaveMode.Overwrite)
          .csv("C:\\Users\\Nathaniel\\Dossier principal\\Bethel_Info_Service\\Formation Juvénal Data Engenieur\\Ressources\\FichiersBANO\\bano_" + x.toString)

        val chemin_source = new Path("C:\\Users\\Nathaniel\\Dossier principal\\Bethel_Info_Service\\Formation Juvénal Data Engenieur\\Ressources\\FichiersBANO\\bano_" + x.toString)

    fs.copyFromLocalFile(chemin_source, chemin_dest)
    }
    /*
    df_departement.foreach {
      dep =>
        df_bano.filter(col("code_departement") === dep.toString())
          .repartition(1)
          .write
          .format("com.databricks.spark.csv")
          .option("delimiter", ";")
          .option("header", "true")
          .mode(SaveMode.Overwrite)
          .csv("C:\\Users\\Nathaniel\\Dossier principal\\Bethel_Info_Service\\Formation Juvénal Data Engenieur\\Ressources\\FichiersBANO\\bano_" + dep.toString)
        val chemin_source = new Path("C:\\Users\\Nathaniel\\Dossier principal\\Bethel_Info_Service\\Formation Juvénal Data Engenieur\\Ressources\\FichiersBANO\\bano_" + dep.toString)
        fs.copyFromLocalFile(chemin_source, chemin_dest)
    }*/

  }
}
