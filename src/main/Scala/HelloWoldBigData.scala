import scala.collection.mutable._
import scala.tools.nsc.doc.model.Public

object HelloWoldBigData {
  /* premier programme */
  val ma_var_imm : String = "Florent GOKOU" // variable immutable = constante
  private val une_var_imm : String =  "Formation Big Data" // variable à portée privée

  class person(
                var nom : String,
                var prenom : String,
                var age : Int
              )

  def main(arg: Array[String]) :Unit = {
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
  def Comptage_caracteres(texte : String) : Int = {
    texte.trim.length()

  }

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
}

