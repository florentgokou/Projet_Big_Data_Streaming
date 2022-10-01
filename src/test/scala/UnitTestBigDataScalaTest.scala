// utilisation du modèle de test FunSuite

import org.scalatest
import org.scalatest.funsuite.AnyFunSuite

class UnitTestBigDataScalaTest extends AnyFunSuite {
  test("La Division doit renvoyer 10"){
    assert(HelloWoldBigData.division(20,2) === 10 )
  }

  test("La Division doit renvoyer une erreur de type ArithmeticException") {
    assertThrows[ArithmeticException](
      HelloWoldBigData.division(20, 0)
    )

  }

}
