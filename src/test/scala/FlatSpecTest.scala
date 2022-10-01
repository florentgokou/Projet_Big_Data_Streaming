// utilisation de la suite
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest._
import org.scalatest.matchers.should.Matchers

class FlatSpecTest extends AnyFlatSpec with Matchers {
  "La divistion" should("doit renvoyer 10") in {
    assert(HelloWoldBigData.division(20,2)=== 10)
  }

  "An arithmical error " should("be thrown") in {

    an [ArithmeticException] should be thrownBy (HelloWoldBigData.division(20,0))
  }
  it should("send a OutofBound Error") in {
    var list_fruit: List[String] = List("banane", "pamplemousse", "goyave")
    assertThrows[IndexOutOfBoundsException](list_fruit(4))
  }

  it should("return the starting letters of the string  ") in {
    var chaine : String = "chaine de caractères"
    chaine should startWith("c")
  }

}
