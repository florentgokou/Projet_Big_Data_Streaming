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
  //val fs = fileSystem.get(configH)

  def main(arg: Array[String] ): Unit = {
    val ss = Session_Spark(true)
    ss.sparkContext.setLogLevel("OFF")

    val df_bano_brut = ss.read
      .format("com.databricks.spark.csv")
      .option("delimiter",",")
      .option("header","true")
      .schema(schema_bano)
      .csv("C:\\Users\\Nathaniel\\Dossier principal\\Bethel_Info_Service\\Formation Juvénal Data Engenieur\\Ressources\\FichiersBANO\\full.csv")
    //df_bano_brut.show(10)

    // Transformation
    val df_bano = df_bano_brut
      .withColumn("")
  }

}
