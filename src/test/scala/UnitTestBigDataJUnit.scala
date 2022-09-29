import org.junit._
import org.junit.Assert._

// Effectuer des tests unitaires avec JUnit
class UnitTestBigDataJUnit {

  @Test // annotation qui indique que la fonction qui suit est un test
  def testDivision () : Unit = {
    var valeur_actuelle : Double = HelloWoldBigData.division(10, 3)
    var valeur_prevue : Int = 5
    assertEquals("Le rersultat de cette division normalement est : 5",valeur_prevue, valeur_actuelle.toInt)  // Test d'état
  }

  @Test
  def testConverstion() : Unit = {
  var valeur_actuelle : Int = HelloWoldBigData.convert_entier("13")
  var valeur_prevue : Int = 13
    assertSame("Résultat attendu : La fonction doit renvoyer normalement le nombre ", valeur_actuelle, valeur_prevue)
  }

  @Test
  def testComptageCaractaire(): Unit = {
    var valeur_actuelle: Int = HelloWoldBigData.comptage_caracteres("exemple de caractaires")
    var valeur_prevue: Int = 22
    assertSame("Résultat attendu : La fonction doit renvoyer normalement le nombre ", valeur_actuelle, valeur_prevue)
  }

}
