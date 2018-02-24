package scala.AkkaDM.helloworld

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{Actor, ActorPath, ActorRef, ActorSystem, Props, RootActorPath, Terminated}
import akka.cluster.{Cluster, Member, MemberStatus}
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberEvent, MemberUp}
import akka.io.Udp.Send
import akka.pattern.Patterns
import akka.util.Timeout

import scala.concurrent.duration._
import scala.io.StdIn
import scala.util.{Failure, Success}


final case class TransformationJob(text: String) // 任务内容
final case class TransformationResult(text: String) // 执行任务结果
final case class JobFailed(reason: String, job: TransformationJob) //任务失败相应原因
case object BackendRegistration // 后台具体执行任务节点注册事件

object cluster extends App {


  def main(args : Array[String]) {

    TransformationFrontendApp.main(Seq("2551").toArray)  //启动集群客户端
    TransformationBackendApp.main(Seq("8001").toArray)   //启动三个后台节点
    TransformationBackendApp.main(Seq("8002").toArray)
    TransformationBackendApp.main(Seq("8003").toArray)

    val system = ActorSystem("OTHERSYSTEM")
    val clientJobTransformationSendingActor =
      system.actorOf(Props[ClientJobTransformationSendingActor],
        name = "clientJobTransformationSendingActor")

    val counter = new AtomicInteger
    import system.dispatcher
    system.scheduler.schedule(2.seconds, 2.seconds) {   //定时发送任务
      clientJobTransformationSendingActor ! Send(counter.incrementAndGet())
    }
    StdIn.readLine()
    system.terminate()
  }
}


class TransformationBackend extends Actor {

  val cluster = Cluster(context.system)

  override def preStart(): Unit = cluster.subscribe(self, classOf[MemberEvent])  //在启动Actor时将该节点订阅到集群中
  override def postStop(): Unit = cluster.unsubscribe(self)

  def receive = {
    case TransformationJob(text) => { // 接收任务请求
      val result = text.toUpperCase // 任务执行得到结果（将字符串转换为大写）
      sender() ! TransformationResult(text.toUpperCase) // 向发送者返回结果
    }
    case state: CurrentClusterState =>
      state.members.filter(_.status == MemberStatus.Up) foreach register // 根据节点状态向集群客户端注册
    case MemberUp(m) => register(m)  // 将刚处于Up状态的节点向集群客户端注册
  }

  def register(member: Member): Unit = {   //将节点注册到集群客户端
    context.actorSelection(RootActorPath(member.address) / "user" / "frontend") !
      BackendRegistration
  }
}

class TransformationFrontend extends Actor {

  var backends = IndexedSeq.empty[ActorRef] //任务后台节点列表
  var jobCounter = 0

  def receive = {
    case job: TransformationJob if backends.isEmpty =>  //目前暂无执行任务节点可用
      sender() ! JobFailed("Service unavailable, try again later", job)

    case job: TransformationJob => //执行相应任务
      jobCounter += 1
      implicit val timeout = Timeout(5 seconds)
      val backend = backends(jobCounter % backends.size) //根据相应算法选择执行任务的节点
      println(s"the backend is ${backend} and the job is ${job}")
      val result  = (backend ? job)
        .map(x => x.asInstanceOf[TransformationResult])  // 后台节点处理得到结果
      result pipeTo sender  //向外部系统发送执行结果

    case BackendRegistration if !backends.contains(sender()) =>  // 添加新的后台任务节点
      context watch sender() //监控相应的任务节点
      backends = backends :+ sender()

    case Terminated(a) =>
      backends = backends.filterNot(_ == a)  // 移除已经终止运行的节点
  }
}
class ClientJobTransformationSendingActor extends Actor {

  val initialContacts = Set(
    ActorPath.fromString("akka.tcp://ClusterSystem@127.0.0.1:2551/system/receptionist"))
  val settings = ClusterClientSettings(context.system)
    .withInitialContacts(initialContacts)

  val c = context.system.actorOf(ClusterClient.props(settings), "demo-client")


  def receive = {
    case TransformationResult(result) => {
      println(s"Client response and the result is ${result}")
    }
    case Send(counter) => {
      val job = TransformationJob("hello-" + counter)
      implicit val timeout = Timeout(5 seconds)
      val result = Patterns.ask(c,ClusterClient.Send("/user/frontend", job, localAffinity = true), timeout)
      result.onComplete {
        case Success(transformationResult) => {
          self ! transformationResult
        }
        case Failure(t) => println("An error has occured: " + t.getMessage)
      }
    }
  }
}
