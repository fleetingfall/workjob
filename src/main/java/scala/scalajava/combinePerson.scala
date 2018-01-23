package scala.scalajava

object combinePerson extends App {
  def test: Unit ={
    val person:Person=new Person("kingcall",20)
    /*在这里相互调用的时候出错了*/
    person.getMessage()
  }

}
