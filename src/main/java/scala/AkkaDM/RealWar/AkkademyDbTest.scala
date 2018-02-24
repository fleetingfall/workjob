package scala.AkkaDM.RealWar
import scala.concurrent.duration._
import akka.actor.ActorSystem
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.{FunSpecLike, Matchers}

class AkkademyDbSpec extends FunSpecLike with Matchers {
  /*implicit val system = ActorSystem()
  implicit val timeout = Timeout(5 seconds)
  describe("akkademyDb") {
    describe("given SetRequest") {
      it("should place key/value into map") {
        val actorRef = TestActorRef(new AkkademyDb)
        actorRef ! SetRequest("key", "value")
        val akkademyDb = actorRef.underlyingActor
        akkademyDb.map.get("key") should equal(Some("value"))
      }
    }
  }*/
}
