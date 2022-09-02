//import org.apache.log4j._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

object SparkBigData {

  // Developpement d'applications Big Data en Spark
  var ss : SparkSession = null

  def main(args: Array[String]): Unit = {
    val sc = Session_Spark(Env = true).sparkContext

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

        //rdd3.saveAsTextFile(path ="C:\\Users\\Nathaniel\\Dossier principal\\Bethel_Info_Service\\Formation Juvénal Data Engenieur\\Fichiers_Data\\rdd3.txt")
        //rdd3.repartition(numPartitions = 1).saveAsTextFile(path ="C:\\Users\\Nathaniel\\Dossier principal\\Bethel_Info_Service\\Formation Juvénal Data Engenieur\\Fichiers_Data\\rdd3_V2.txt")
        //rdd3.collect().foreach( l => println(l))
        print("\n")
        //println("Affichage du contenu du fichier : TextRDD.txt")
        // Création d'un RDD à partir d'une source de données ( à revoir)
        //val rdd4 = sc.textFile( path = "C:\\Users\\Nathaniel\\Dossier principal\\Bethel_Info_Service\\Formation Juvénal Data Engenieur\\Fichiers_Access\\TextRDD.txt")
        //println("lecture du contenu du rdd4")
        //rdd4.foreach{l => println(l)}
        print("\n")
        println("Affichage du contenu d'un lot de fichiers ou d'un repertoir")
        // Création d'un RDD à partir d'une source de données ( à revoir)
        //val rdd5 = sc.textFile(path = "C:\\Users\\Nathaniel\\Dossier principal\\Bethel_Info_Service\\Formation Juvénal Data Engenieur\\Fichiers_Access\\*")
        //println("lecture du contenu du rdd5")
        //rdd5.foreach{l => println(l)}

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
      rdd_compte.repartition(1).saveAsTextFile("C:\\Users\\Nathaniel\\Dossier principal\\Bethel_Info_Service\\Formation Juvénal Data Engenieur\\Fichiers_Access\\rdd_compte.txt")
      rdd_compte.foreach(l =>println(l))

      print("\n")
      println("val rdd_filtered = rdd_fm.filter(x => x._1.contains(\"banane\"))")
      val rdd_filtered = rdd_fm.filter(x => x._1.contains("banane"))
      rdd_filtered.foreach(l => println(l))

      println("\n")
      println("val rdd_reduced = rdd_fm.reduceByKey((x,y) => x + y )")
      val rdd_reduced = rdd_fm.reduceByKey((x, y) => x + y )
      //rdd_reduced.repartition(1).saveAsTextFile("C:\\Users\\Nathaniel\\Dossier principal\\Bethel_Info_Service\\Formation Juvénal Data Engenieur\\Fichiers_Access\\rdd_reduced.txt")
      rdd_reduced.foreach(l => println(l))
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
          .config("spark.sql.crossJoin.enable", "true")
          //.enableHiveSupport()
          .getOrCreate()
      } else {
        ss = SparkSession.builder()
          .appName(name = "Mon application Spark")
          .config("spark.serializer", "org.apache.spark.serializer.kryoSerializer")
          .config("spark.sql.crssJoin.enabled", "true")
          //.enableHiveSupport()
          .getOrCreate()
      }
      ss
    }
}
