package scala.classDemo

object Test {
  def main(args: Array[String]): Unit = {
    val counter=new MyCounter
    println(counter.incerece())
    println(counter.value)

    val p=new GetSetCounter()
    println(p.getValue)
    p.setValue(10)
    println(p.getValue)


    val con=new ConstruceCounter2("kingcall",22)
    println(con.age)
    con.age=28
    println(con.age)
  }

}
