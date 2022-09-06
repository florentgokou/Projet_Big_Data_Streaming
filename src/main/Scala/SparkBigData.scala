//import org.apache.log4j._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
object SparkBigData {

  // Developpement d'applications Big Data en Spark
  var ss : SparkSession = null
  //var spConf : SparKConf = null

  def main(args: Array[String]): Unit = {
    val session_s = Session_Spark(Env = true)
    val sc = session_s.sparkContext

    sc.setLogLevel("OFF")
    println("Contenu de : \\DataFrame\\2010-12-06.csv")
    val def_test = session_s.read
      .format("com.databricks.sparck.csv")
      .option("delimiter",",")
      .option("header","true")
      .csv("C:\\Users\\Nathaniel\\Dossier principal\\Bethel_Info_Service\\Formation Juvénal Data Engenieur\\Ressources\\DataFrame\\2010-12-06.csv")
    //def_test.show(numRows = 15)
    def_test.printSchema()

    val def_2 = def_test.select(
      col("InvoiceNo").cast(StringType),
      col("_C0").alias(" ID_DU_CLEINT"),
      col("StockCode").cast(IntegerType).alias("Code_de_la_Marchandise"),
      col("Invoice".concat("No")).alias("ID_DE_LA_CMDE")
    )
    def_2.show(numRows =10)

    println("df_3 = def_test.withColumn(\"InvoiceNo...")
    val df_3 = def_test.withColumn("InvoiceNo", col("InvoiceNo").cast(StringType))
      .withColumn("StockCode", col("StockCode").cast(IntegerType))
      .withColumn("valeur_constante", lit(50))
      .withColumnRenamed("_c0", "ID_client")
      .withColumn("ID_commande", concat_ws("|", col("InvoiceNo"), col("ID_client")))
      .withColumn("total_amount", round(col("Quantity") * col("UnitPrice"), 2))
      .withColumn("Created_dt", current_timestamp())
      .withColumn("reduction_test", when(col("total_amount") > 15, lit(3)).otherwise(lit(0)))
      .withColumn("reduction", when(col("total_amount") < 15, lit(0))
        .otherwise(when(col("total_amount").between(15, 20), lit(3))
          .otherwise(when(col("total_amount") > 15, lit(4)))))
      .withColumn("net_income", col("total_amount") - col("reduction"))

    df_3.show(5)

    println("Filtre : val df_notreduced = df_3.filter(col(\"reduction\") ===")
    val df_notreduced = df_3.filter(col("reduction") === lit(0) && col("Country").isin("United Kingdom", "France", "USA"))
    df_notreduced.show( 3)


    println("Contenu de :  \\Ressources\\DataFrame\\CSV\\")
    val def_gp = session_s.read
      .format("csv")
      .option("header", "true")
      .option("inferShema","true")
      .load("C:\\Users\\Nathaniel\\Dossier principal\\Bethel_Info_Service\\Formation Juvénal Data Engenieur\\Ressources\\DataFrame\\CSV\\")
    //def_gp.show(numRows = 15)
    println("def_test count : " + def_test.count() + " def_gp count : " + def_gp.count())

    println("Contenu de :  \\Ressources\\DataFrame\\CSV\\2010-12-06.csv et 2011-01-20.csv")
    val def_gp2 = session_s.read
      .format("csv")
      .option("header", "true")
      .option("inferShema","true")
      .load("C:\\Users\\Nathaniel\\Dossier principal\\Bethel_Info_Service\\Formation Juvénal Data Engenieur\\Ressources\\DataFrame\\2011-01-20.csv" , "C:\\Users\\Nathaniel\\Dossier principal\\Bethel_Info_Service\\Formation Juvénal Data Engenieur\\Ressources\\DataFrame\\2010-12-06.csv")
    //def_gp2.show(numRows = 7)
    println("def_test count : " + def_test.count() + " def_gp count : " + def_gp.count() + " def_gp2 count : " + def_gp2.count())

    //manip_rdd()
  }

  def manip_rdd(): Unit = {
    val session_s = Session_Spark(Env = true)
    val sc = session_s.sparkContext

    sc.setLogLevel("OFF")
    println("Affichage par la méthose : List")
    val rdd_test: RDD[String] = sc.parallelize(List("alain", "florent", "julien", "yechoua"))
    rdd_test.foreach {
      l =>
        println(l)
    }

    print("\n")
    println("Affichage par la méthose : Array")
    val rdd2: RDD[String] = sc.parallelize(Array("Lucie", "Fabien", "Jules", "Nathaniel"))
    rdd2.foreach {
      l => println(l)
    }
    print("\n")
    println("Affichage par la méthose : Seq")
    val rdd3 = sc.parallelize(Seq(("Lucie", 34, "grand frère", "Le fils"), (300, 900, "Eglise"), ("Jules", 98, "Nathaniel")))
    rdd3.take(num = 1).foreach {
      l => println(l)
    }

    print("\n")
    println("Affichage par la méthose : rdd3.isEmpty()")
    if (rdd3.isEmpty()) {
      println("le RDD3 est vide")
    } else {
      rdd3.foreach(l => println(l))
    }
    println("Exécution de : rdd3.saveAsTextFile(path =C:\\Users\\...")
    rdd3.saveAsTextFile(path ="C:\\Users\\Nathaniel\\Dossier principal\\Bethel_Info_Service\\Formation Juvénal Data Engenieur\\Fichiers_Data\\rdd3.txt")
    //rdd3.repartition(numPartitions = 1).saveAsTextFile(path ="C:\\Users\\Nathaniel\\Dossier principal\\Bethel_Info_Service\\Formation Juvénal Data Engenieur\\Fichiers_Data\\rdd3_V2.txt")
    println("Conetu de : rdd3.collect().foreach( l => println(l)) - rdd3.repartition(numPartitions = 1).saveAsTextFile(path =\"C:\\\\Users\\...")
    rdd3.collect().foreach( l => println(l))

    print("\n")
    println("Affichage du contenu du fichier : TextRDD.txt  -- val rdd4 = sc.textFile( path = ) ")
    // Création d'un RDD à partir d'une source de données ( à revoir)
    //val rdd4 = sc.textFile( path = "C:\\Users\\Nathaniel\\Dossier principal\\Bethel_Info_Service\\Formation Juvénal Data Engenieur\\Fichiers_Access\\TextRDD.txt")
    println("lecture du contenu du rdd4")
    //rdd4.foreach{l => println(l)}

    print("\n")
    println("Affichage du contenu d'un lot de fichiers ou d'un repertoir // // Erreur a été corrigé")
    // Création d'un RDD à partir d'une source de données ( à revoir)  // Erreur à corriger
    val rdd5 = sc.textFile(path = "C:\\Users\\Nathaniel\\Dossier principal\\Bethel_Info_Service\\Formation Juvénal Data Engenieur\\Fichiers_Access\\*")
    println("lecture du contenu du rdd5 : \\Formation Juvénal Data Engenieur\\Fichiers_Access\\*")
    rdd5.foreach{l => println(l)}

    print("\n")
    println("Affichage de données transformées - Transformation des RDD")
    // Transformation des RDD
    val rdd_trans: RDD[String] = sc.parallelize(List("Yechoua joue au balon","Alain mange une banane", "La banane est un bon aliment pour la santé", "Achater une bonne banane"))
    rdd_trans.foreach(l => println("Ligne de mon RDD : " + l))

    val rdd_map = rdd_trans.map(x => x.split(""))
    println("Nombre d'éléments de mon RDD Map :"  + rdd_map.count())

    //val rdd6 = rdd_trans.map(w => (w, w.length))
    val rdd6 = rdd_trans.map(w => (w, w.length, w.contains("banane")))
    rdd6.foreach(l => println(l))

    val rdd7 = rdd6.map(x => (x._1.toUpperCase(),x._2, x._3) )
    rdd7.foreach(l => println(l))

    print("\n")
    println("val rdd8 = rdd6.map(x => (x._1.split(\" \"),1 ))")
    val rdd8 = rdd6.map(x => (x._1.split(" "),1 ))
    rdd8.foreach(l => println(l._1(0),l._2))

    print("\n")
    println("val rdd_fm = rdd_trans.flatMap(x => x.split(\"\")).map(w => (w,1))")
    val rdd_fm = rdd_trans.flatMap(x => x.split(" ")).map(w => (w,1))
    rdd_fm.foreach(l => println(l))

    print("\n")
    println("val rdd_compte = rdd_trans.flatMap(x => x.split(\"\")).map(m => (m,1))")
    val rdd_compte = rdd_trans.flatMap(x => x.split(" ")).map(m => (m,1))
    //rdd_compte.repartition(1).saveAsTextFile("C:\\Users\\Nathaniel\\Dossier principal\\Bethel_Info_Service\\Formation Juvénal Data Engenieur\\Fichiers_Access\\rdd_compte.txt")
    rdd_compte.foreach(l =>println(l))

    print("\n")
    println("val rdd_filtered = rdd_fm.filter(x => x._1.contains(\"banane\"))")
    val rdd_filtered = rdd_fm.filter(x => x._1.contains("banane"))
    rdd_filtered.foreach(l => println(l))

    println("\n")
    println("val rdd_reduced = rdd_fm.reduceByKey((x,y) => x + y ) - Erreur à été corrigé")  //
    val rdd_reduced = rdd_fm.reduceByKey((x, y) => x + y )  // Erreur à corriger
    rdd_reduced.repartition(1).saveAsTextFile("C:\\Users\\Nathaniel\\Dossier principal\\Bethel_Info_Service\\Formation Juvénal Data Engenieur\\Fichiers_Access\\rdd_reduced.txt")
    println("Affichage du contenu de rdd_reduced")
    rdd_reduced.foreach(l => println(l))

    println("\n")
    println("rdd_fm.persist(StorageLevel.MEMORY_AND_DISK)")
    //rdd_fm.cache()
    //rdd_fm.persist(StorageLevel.MEMORY_AND_DISK)
    //rdd_fm.unpersist()

    println("\n")
    println("val df : DataFrame = rdd_fm.toDF(colNames = \"text\",\"valeur\")")
    import session_s.implicits._
    val df : DataFrame = rdd_fm.toDF(colNames = "text","valeur")
    df.show(numRows = 50)
  }

  /**
   * fonction qui initialise et instancie une session spark
   *
   * @param env : c'est une variable qui indique l'environnement sur lequel notre application est  déployée
   *            si env = True, alors l'application est déployée en local, sinon elle est déployée sur un cluster
   */

  def Session_Spark(Env: Boolean = true): SparkSession = {
    if (Env == true) {
      System.setProperty("hadoop.home.dir", "c:/Hadoop/")
      ss = SparkSession.builder
        .master(master = "local[*]")
        .config("spark.sql.crossJoin.enabled", "true")
        //.enableHiveSupport()
        .getOrCreate()
    } else {
      ss = SparkSession.builder()
        .appName(name = "Mon application Spark")
        .config("spark.serializer", "org.apache.spark.serializer.kryoSerializer")
        .config("spark.sql.crossJoin.enabled", "true")
        //.enableHiveSupport()
        .getOrCreate()
    }
    ss
  }
}