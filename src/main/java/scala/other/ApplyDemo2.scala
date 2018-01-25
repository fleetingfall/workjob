package scala.other

class ApplyDemo2 (val id:Int,initbalence:Double){
  private var balence=initbalence
}

object ApplyDemo2{
  def apply(id:Int,initbalence:Double) = new ApplyDemo2(id, initbalence)
}
