package scala.javascala

import org.apache.hadoop.classification.InterfaceAudience.Public

import scala.beans.BeanProperty

class Person(@BeanProperty val name:String, var age:Int) {

  def getMesaage(): String ={
    "姓名:"+name+",年龄:"+age
  }

}
