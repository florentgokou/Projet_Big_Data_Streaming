import scala.collection.mutable._
import scala.tools.nsc.doc.model.Public
import org.apache.log4j._
import java.io.FileNotFoundException
import scala.io._

import PackageTest.PBGS.ClassTest

object HelloWoldBigData extends ClassTest {
  /* premier programme */
  val ma_var_imm : String = "Florent GOKOU" // variable immutable = constante
  private val une_var_imm : String =  "Formation Big Data" // variable à portée privée

  /**
   *
   * @param nom
   * @param prenom
   * @param age
   */
  class person( var nom : String, var prenom : String, var age : Int )
  BasicConfigurator.configure()
  private var trace_appli : Logger = LogManager.getLogger("Logger_Consol")

  /**
   *
   * @param arg
   */
  def main(arg: Array[String]) :Unit = {

    val tt : Int = comptage_package("essai")

    val diviseur : Double = try{
      division(12,3)
    } catch {
      case ex : ArithmeticException => 0
      case ex2 : IllegalArgumentException => 0
    }
    trace_appli.info(s"La valeur de votre division est de : ${diviseur}")

    lecture_fichier("C:\\Dossier principal\\Bethel_Info_Service\\Formation Juvénal Data Engenieur\\Ressources\\DataFrame\\orders.txt")

    println("Logger de convert_entier")
    val nombre : Int = convert_entier("100")
    trace_appli.info(s"La valeur de votre nombre converti est : ${nombre}")
    println("\n")
    println("Logger de  Comptage_caracteres")
    val Nombre_carataire : Int = comptage_caracteres("GOKOU")
    trace_appli.info(s"Nombre de carataires est : ${Nombre_carataire}")

    //print("Hello Wold : Mon premier programme en Scala\n")
    var test_mu: Int = 15  // variable mutable
    test_mu = test_mu + 10
    //println(test_mu)

    val test_imm : Int = 15
    //println(test_imm)
    //println("Votre Testet contient : " + Comptage_caracteres(texte = "Jésus est merveilleux")+ " Caractères")

    //testWhile(valeur_cond = 8)

    //testfor()

    //collectionScala()
    collectionTuple()

  }


  // ma première fonction
  println("\n")
  println("Comptage_caracteres - Démarrage du traçage de la classe)")

  /**
   *
   * @param texte
   * @return
   */
  def comptage_caracteres(texte : String) : Int = {
    //texte.trim.length()
    trace_appli.info("Démarrage du traçage de la classe")
    trace_appli.info(s"Le paramètre tracé par Log4j pour cette fonction est : $texte")
    trace_appli.warn(s"Message d'avertissement Loj4J interpolation de chaine : ${10 + 15}")
    if (texte.isEmpty){
      0
    } else{
      texte.trim.length()
    }
  }
  println("fin du traçage de la classe)")


  // structure conditionnelle
  def testWhile(valeur_cond: Int): Unit = {
    var i: Int = 0
    while (i < valeur_cond) {
      println("Itération While N : " + i)
      i = i + 1
    }
  }
  def testfor() : Unit = {
    var i: Int = 0
    for (i <- 5 to 15) {
      println("Itération for N : " + i)
    }
  }

  // Les collection en Scala
  def collectionScala(): Unit = {

    val maliste: List[Int] = List(1, 2, 3, 10, 45, 15)
    val liste_S: List[String] = List("Florent", "Yechoua", "Eloïse", "Syntyche", "Nathaniel", "Julienne", "Prsicille")
    val plage_v: List[Int] = List.range(1, 15, 2)

    println("Résulta de la focntion maliste")
    println(maliste(0))
    println("Résulta de la focntion liste_S(1)")
    println(liste_S(1))
    println("Résulta de la focntion plage_v")
    println(plage_v(4))

    println("Tout le contenu de liste_S")
    for (i <- liste_S) {
      println(i)
    }
    // manipulation des collection à l'aide des fonctions anonymes
    println("\nAffichage de Tous les éléments de liste_S qui termine par 'e' ")
    val resutats: List[String] = liste_S.filter(e => e.endsWith("e"))
    for (r <- resutats) {
      println(r)
    }
    val res: Int = liste_S.count(i => i.endsWith("e"))
    println(" Nombre d'éléments qui termione par 'e' : " + res)

    val maliste2: List[Int] = maliste.map(e => e * 2)
    for (r <- maliste2) {
      println(r)
    }

    val maliste3: List[Int] = maliste.map((e: Int) => e * 2)
    val maliste4: List[Int] = maliste.map(_ * 2)

    val nouvelle_liste: List[Int] = plage_v.filter(p => p > 5)
    val new_list: List[String] = liste_S.map(s => s.capitalize)

    nouvelle_liste.foreach(e => println("nouvelle liste : " + e))
    new_list.foreach(e => println("new_list : " + e))
  }
    def collectionTuple() : Unit = {
      val tuple_test = (45, "JVC", "Gokou","False")
      println(tuple_test._4)

      val nouv_personne : person = new person(nom="Gokou", prenom = "Florent", age = 25)
      val tuple_2 = ("teest", nouv_personne, 67)
      tuple_2.toString().toString
    }
// table de hachage
  val states = Map(
    "AK" -> "Alaska",
    "IL" -> "Illinois",
    "KY" -> "Kentucky"
  )

  val peronne = Map(
    "nom" -> "GOKOU",
    "prénom" -> "Florent",
    "age" -> 45
  )

  // Les Tableau ou Array
  val montableau : Array[String] = Array("jvc","Florent","test")
  montableau.foreach(e => println(e))

  //Gérer les exceptions avec Try{} Catch{} Finally {}
  println("\n")
  println("Utilisation d'un gestionnaire d'erreur")
  def convert_entier (nombre_chaine : String) : Int = {
    try {
      val nombre: Int = nombre_chaine.toInt
      return nombre
    } catch {
          //trace_appli.error("La fonction agénérée une erreur. LA valeur 0 a été attribuée par défaut")
      case ex: Exception => 0
    }
  }

  def lecture_fichier(chemin_fichier: String): Unit = {
    try {
      val fichier = Source.fromFile(chemin_fichier)
      fichier.getLines()
      fichier.close()
      trace_appli.info("Votre fichier a été retrouvé")
    } catch {
      case ex : FileNotFoundException => trace_appli.error("Votre fichier est introuvable. Vérifier le chemin d'accès" + ex.printStackTrace())
    }
  }
  println("Fonction Division")
  def division(numerateur : Int, denominateur : Int) : Double = {
  val resultat = numerateur/denominateur
    return resultat
  }
}

