import org.apache.hadoop.fs._
import org.apache.log4j._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.streaming._
import java.io.FileNotFoundException
import org.apache.spark.sql.expressions.Window.partitionBy

object SparkBigData {

  // Developpement d'applications Big Data en Spark
  var ss : SparkSession = null
  //var spConf : SparKConf = null

  private var trace_log : Logger = LogManager.getLogger("Logger_Consol")

  val schema_order = StructType(Array(
    StructField("orderid", IntegerType, false),
    StructField("customerid", IntegerType, false),
    StructField("campaignid", IntegerType, true),
    StructField("orderdate", TimestampType, true),
    StructField("city", StringType, true),
    StructField("state", StringType, true),
    StructField("zipcode", StringType, true),
    StructField("paymenttype", StringType, true),
    StructField("totalprice", DoubleType, true),
    StructField("numorderlines", IntegerType, true),
    StructField("numunits", IntegerType, true)
    )
  )

  def main(args: Array[String]): Unit = {
    val session_s = Session_Spark(Env = true)
    val sc = session_s.sparkContext

    sc.setLogLevel("OFF")
    println("Contenu de : \\DataFrame\\2010-12-06.csv")
    val def_test = session_s.read
      .format("com.databricks.sparck.csv")
      .option("delimiter",",")
      .option("header","true")
      .csv("D:\\Dossier principal\\Bethel_Info_Service\\Formation Juvénal Data Engenieur\\Ressources\\DataFrame\\2010-12-06.csv")
    //def_test.show(numRows = 15)
    //def_test.printSchema()

    val def_2 = def_test.select(
      col("InvoiceNo").cast(StringType),
      col("_C0").alias(" ID_DU_CLEINT"),
      col("StockCode").cast(IntegerType).alias("Code_de_la_Marchandise"),
      col("Invoice".concat("No")).alias("ID_DE_LA_CMDE")
    )
    //def_2.show(numRows =10)

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

    //df_3.show(5)

    println("Filtre : val df_notreduced = df_3.filter(col(\"reduction\") ===")
    val df_notreduced = df_3.filter(col("reduction") === lit(0) && col("Country").isin("United Kingdom", "France", "USA"))
    //df_notreduced.show( 3)

    // Jointure de la DataFrame
    println("\n")
    println("Contenu de orders.txt avec val df_orders = session_s.read")
    val df_orders = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .schema(schema_order)
      .load("D:\\Dossier principal\\Bethel_Info_Service\\Formation Juvénal Data Engenieur\\Ressources\\DataFrame\\orders.txt")
    df_orders.printSchema()
    //df_orders.show(numRows = 15)

    println("val df_ordersGood =  df_orders.withColumnRenamed")
     val df_ordersGood =  df_orders.withColumnRenamed("numunits", "numunits_order")
      .withColumnRenamed("totalprice", "totalprice_order")
       .withColumnRenamed("orderid", "orderid_order")
    //df_ordersGood.printSchema()

    println("val df_products = session_s.read")
    val df_products = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .load("D:\\Dossier principal\\Bethel_Info_Service\\Formation Juvénal Data Engenieur\\Ressources\\DataFrame\\product.txt")

    println("val df_productsGood =  df_products.withColumnRenamed")
    val df_productsGood =  df_products.withColumnRenamed("PRODUCTID", "PRODUCTID_PRD")

    println("val df_orderslines = session_s.read")
    val df_orderslines = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .load("D:\\Dossier principal\\Bethel_Info_Service\\Formation Juvénal Data Engenieur\\Ressources\\DataFrame\\orderline.txt")

     val de_joinOrders = df_orderslines.join(df_ordersGood, df_ordersGood.col("orderid_order") === df_orderslines.col("orderid"), "inner")
       .join(df_productsGood, df_productsGood.col("PRODUCTID_PRD") === df_orderslines.col("PRODUCTID"), Inner.sql)

    println("\n")
    //de_joinOrders.printSchema()

    println("Union des trois fichiers 2010-12-06.csv - 2011-01-20.csv - 2011-12-08.csv")
    val def_fichier1 = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .csv("D:\\Dossier principal\\Bethel_Info_Service\\Formation Juvénal Data Engenieur\\Ressources\\DataFrame\\2010-12-06.csv")

    val def_fichier2 = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .csv("D:\\Dossier principal\\Bethel_Info_Service\\Formation Juvénal Data Engenieur\\Ressources\\DataFrame\\2011-01-20.csv")

    val def_fichier3 = session_s.read
      .format("com.databricks.spark.csv")
        .option("delimiter", ",")
      .option("header", "true")
      .csv("D:\\Dossier principal\\Bethel_Info_Service\\Formation Juvénal Data Engenieur\\Ressources\\DataFrame\\2011-12-08.csv")

      val df_unitedfiles = def_fichier1.union(def_fichier2.union(def_fichier3))
      //println(def_fichier3.count() + " "+ df_unitedfiles.count())

      // Manipulations avancées de data frames : agrégats et fenêtrage
    println("Manipulations avancées de data frames : agrégats et fenêtrage")
    de_joinOrders.withColumn("Total_amount", round(col("numunits")*col("totalprice_order"), scale=3))
      .groupBy("city")
      .sum("Total_amount").alias("Commande_Total")
      //.show()


    // Opération de fenêtrage
    println("Opération de fenêtrage")
    //val wn_spec = Window.partitionBy(col("state"))
    val wn_spec = partitionBy(col("state"))
    val def_windows = de_joinOrders.withColumn("ventes_dep",sum(round(col("numunits") * col("totalprice_order"), scale=3)).over(wn_spec))
      .select(
        col("orderlineid"),
        col("orderid_order"),
        col("PRODUCTID_PRD"),
        col("state"),
        round(col("ventes_dep"), scale=3).alias("Vente_par_département")
      )//.show(10)

    println("Manipulation des dates et du temps en Spark")
    //orderdate: string (nullable = true)
    df_ordersGood.withColumn("date_lecture",date_format(current_date(),"dd/MMMM/yyyy hh:mm:ss" ) )
      .withColumn("date_lecture_complete",current_timestamp())
      //.withColumn("periode_secondes",window(col("orderdate"),"5 seconds"))
      .withColumn("periode_jour",window(col("orderdate"),"5 days"))
      .select(
        /*col("periode_secondes"),
        col("periode_secondes.start"),
        col("periode_secondes.end") */
        col("periode_jour"),
        col("periode_jour.start"),
        col("periode_jour.end")
      )

      df_unitedfiles.withColumn("InvoiceDate", to_date(col("InvoiceDate")))
        .withColumn("InvoiceTimestamp", col("InvoiceTimestamp").cast(TimestampType))
        .withColumn("Invoice_add_2months",add_months(col("InvoiceDate"),2))
        .withColumn("Invoice_add_date",date_add(col("InvoiceDate"),30))
        .withColumn("Invoice_sub_date",date_sub(col("InvoiceDate"),25))
        .withColumn("Invoice_date_diff",datediff(current_date(), col("InvoiceDate")))
        .withColumn("InvoiceDateQuarter",quarter(col("InvoiceDate")))
        .withColumn("InvoiceDate_id",unix_timestamp(col("InvoiceDate")))
        .withColumn("InvoiceDate_format",from_unixtime(unix_timestamp(col("InvoiceDate")),"dd-MM-yyyy"))

    // Methode de validation de données - .count()
    println("\n")
    println("Methode de validation de données .count()")
    df_products
      .withColumn("productGp",substring(col("PRODUCTNAME"), pos = 2, len = 2))
      .withColumn("productln", length(col("PRODUCTGROUPNAME")))
      .withColumn("concat_product", concat_ws("|", col("PRODUCTID"), col("INSTOCKFLAG")))
      .withColumn("PRODUCTGROUPCODEMIN", lower(col("PRODUCTGROUPCODE")))
      //.where(regexp_extract(col("PRODUCTID")), "[0-9]{5}",0) trim(col("PRODUCTID"))
      .where(!col("PRODUCTID").rlike("[0-9]{5}"))
      //.count()
      //.show(10)

   /* import session_s.implicits._
    val phone_list : DataFrame = list("09102998282","0910293735","+0810299828").toDF("phone_number") // à coriger
    phone_list
      .withColumn("test_phone",valid_phoneUDF(col("phone_number")))
      .show()
*/
    println("\n")
    //df_ordersGood.show(10)
  /*
    //Persister les data frame sur disque/HDFS -- Sur le DD
    def_windows
      .repartition(1)
      .write
      .format("com.databricks.spark.csv")
      .mode(SaveMode.Overwrite)
      .option("header","true")
      .csv("C:\\Users\\Nathaniel\\Dossier principal\\Bethel_Info_Service\\Formation Juvénal Data Engenieur\\Ressources\\DataFrame\\Ecriture")

    // Exemple de propriété d'un format
      def_2.write
      .option("orc.bloom.filter.columns","favorite_color")
      .option("orc.dictionary.key.threshold","1.0")
      .option("orc.column.encoding.direct","name")
      .orc("users_with_option_orc")
  */

    // Exécution du SQL sans le HiveQL
    println("// Exécution du SQL sans le HiveQL")
    de_joinOrders.createTempView("orders")

    // SQL de Spark
    val df_sql = session_s.sql("""
      select state, city, sum( round(numunits * totalprice )) as Commande_Totales from orders group by state, city
    """).show()

    // C'est la mâme requête en RDD - DataFrame
    de_joinOrders.withColumn("total_amount",round(col("numunits") * col("totalprice"), 3))
      .groupBy("state","city")
      .sum("total_amount").as("Commande Totales")
      .show()

    // Ce sont des opérations qui se font beaucoup. Mais dans notre cas, ça ne va pas marcher car on n'a pas de MetaStore
    /*
    val df_hive = session_s.table("Orders") // Lire une table à partir du metastore Hive
    df_sql.write.mode(SaveMode.Overwrite).saveAsTable("report_orders") // enregistrer et écrire un Dataframe dans un DataStore Hive
    */

    //Persister les data frame sur disque/HDFS  -- Sur HDFS
    println("Contenu de :  \\Ressources\\DataFrame\\CSV\\")
    val def_gp = session_s.read
      .format("csv")
      .option("header", "true")
      .option("inferShema","true")
      .load("D:\\Dossier principal\\Bethel_Info_Service\\Formation Juvénal Data Engenieur\\Ressources\\DataFrame\\CSV\\")
    //def_gp.show(numRows = 15)
    println("def_test count : " + def_test.count() + " def_gp count : " + def_gp.count())

    println("Contenu de :  \\Ressources\\DataFrame\\CSV\\2010-12-06.csv et 2011-01-20.csv")
    val def_gp2 = session_s.read
      .format("csv")
      .option("header", "true")
      .option("inferShema","true")
      .load("D:\\Dossier principal\\Bethel_Info_Service\\Formation Juvénal Data Engenieur\\Ressources\\DataFrame\\2010-12-06.csv")
    //def_gp2.show(numRows = 7)
    println("def_test count : " + def_test.count() + " def_gp count : " + def_gp.count() + " def_gp2 count : " + def_gp2.count())

    // Exécution des autres fonctions

    //manip_rdd()
  }
/*
  //Savoir construire et enregistrer des UDF
  def valid_phone(phone_to_test : String) : Boolean = {
    var result :  Boolean = false
    val motif_regexp = "^0[0-9]{9}".r

    if (motif_regexp.findAllIn(phone_to_test.trim) == phone_to_test.trim) {
      result = true
    } else {
      result = false
    }
    return result
  }
  val valid_phoneUDF : UserDefinedFunction = udf{(phone_to_test : String) => valid_phone(phone_to_test : String)}
*/
  def Spark_hdfs() : Unit  = {
    val config_fs = Session_Spark(true).sparkContext.hadoopConfiguration
    val fs = FileSystem.get(config_fs)

    val src_pth = new Path("/user/dtalake/marketing/")
    val desc_pth = new Path("/user/dtalake/indexes/")
    val ren_src = new Path("/user/dtalake/marketing/fichier_reporting.parquet")
    val dest_src = new Path("/user/dtalake/marketing/reporting.parquet")
    val local_path = new Path("D:\\Dossier principal\\Bethel_Info_Service\\Formation Juvénal Data Engenieur\\Ressources\\DataFrame\\Ecriture\\parts.csv")
    val path_local = new Path("D:\\Dossier principal\\Bethel_Info_Service\\Formation Juvénal Data Engenieur\\Ressources\\DataFrame")

    // Lesture des fichiers d'un dossier
    val files_list = fs.listStatus(src_pth).map(x => x.getPath)
    for(i <- 1 to files_list.length) {
      println(files_list)
    }

    // Renommage des fichiers
    fs.rename(ren_src,dest_src)

    //Supprimer des fichiers dans un dossier
    fs.delete(dest_src,true)

    // Copie de fichiers
    fs.copyFromLocalFile(local_path, desc_pth)
    fs.copyToLocalFile(desc_pth, path_local)
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
    rdd3.saveAsTextFile(path ="D:\\Dossier principal\\Bethel_Info_Service\\Formation Juvénal Data Engenieur\\Fichiers_Data\\rdd3.txt")
    //rdd3.repartition(numPartitions = 1).saveAsTextFile(path ="D:\\Dossier principal\\Bethel_Info_Service\\Formation Juvénal Data Engenieur\\Fichiers_Data\\rdd3_V2.txt")
    println("Conetu de : rdd3.collect().foreach( l => println(l)) - rdd3.repartition(numPartitions = 1).saveAsTextFile(path =\"C:\\\\Users\\...")
    rdd3.collect().foreach( l => println(l))

    print("\n")
    println("Affichage du contenu du fichier : TextRDD.txt  -- val rdd4 = sc.textFile( path = ) ")
    // Création d'un RDD à partir d'une source de données ( à revoir)
    val rdd4 = sc.textFile( path = "D:\\Dossier principal\\Bethel_Info_Service\\Formation Juvénal Data Engenieur\\Fichiers_Access\\TextRDD.txt")
    println("lecture du contenu du rdd4")
    rdd4.foreach{l => println(l)}

    print("\n")
    println("Affichage du contenu d'un lot de fichiers ou d'un repertoir // // Erreur a été corrigé")
    // Création d'un RDD à partir d'une source de données   // Erreur à été corriger
    val rdd5 = sc.textFile(path = "D:\\Dossier principal\\Bethel_Info_Service\\Formation Juvénal Data Engenieur\\Fichiers_Access\\*")
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
    rdd_reduced.repartition(1).saveAsTextFile("D:\\Dossier principal\\Bethel_Info_Service\\Formation Juvénal Data Engenieur\\Fichiers_Access\\rdd_reduced.txt")
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
    try {

      if (Env == true) {
        System.setProperty("hadoop.home.dir", "c:/Hadoop/") // à Logger
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
    }
    catch {
      case ex : FileNotFoundException => trace_log.error("Nous n'avons pas trouvé le winutils dans le chemin indiqué" + ex.printStackTrace())
      case ex : Exception => trace_log.error("Erreur dans l'initialisation de la session Spark " + ex.printStackTrace() )
    }
    return  ss
  }

  /**
   * fonction qui initialise le contexte Spark Streaming
   *
   * @param env         : environnement sur lequel est déployé notre application. Si true, alors on est en localhost
   * @param duree_batch : c'est le SparkStreamingBatchDuration - où la durée du micro-batch
   * @return : la fonction renvoie en résultat une instance du contexte Streaming
   */
/*
  def getSparkStreamingContext(env: Boolean = true, duree_batch: Int): StreamingContext = {
    trace_log.info("initialisation du contexte Spark Streaming")
    if (env) {
      spConf = new SparkConf().setMaster("local[*]")
        .setAppName("Mon application streaming")
    } else {
      spConf = new SparkConf().setAppName("Mon application streaming")
    }
    trace_log.info(s"la durée du micro-bacth Spark est définie à : $duree_batch secondes")
    //val ssc: StreamingContext = new StreamingContext(spConf, Seconds(duree_batch))
    val ssc : Int = 0
    return ssc


  }
 */
}