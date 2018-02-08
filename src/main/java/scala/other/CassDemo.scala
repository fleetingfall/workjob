package scala.other

import java.io.{FileNotFoundException, IOException}

/*
* 变量和 _ 都可以解决匹配不上的问题
*
* */
object CassDemo {

  def main(args: Array[String]): Unit = {
    test_judgeIdentify
  }

  /**
    * 变量匹配实例
    */
  def test1(): Unit = {
    val ch: Char = '&'
    val tmp = '+'
    val p = ch match {
      case '+' | '&' => 0
      case '-' => 1
      case '*' => -1
      case '/' => -1
      // 模式变量  在模式中使用变量，而且发现并没有发生掉入下一分支问题
      case tmp => -10
      //避免无匹配出现 MatchError
      case _ => -2
    }
  }
    /*守卫：在满足case 后面的条件后，可以再跟一些条件，进行双重过滤*/
    //看脸（人）的社会
    def judgeGrade(name: String, grade: String) {
      grade match {
        case "A" => println(name + ", you are excellent")
        case "B" => println(name + ", you are good")
        case "C" => println(name + ", you are just so so")
        case _ if name == "leo" => println(name + ", you are a good boy, come on")
        case _ => println("you need to work harder")
      }
    }


    /*
    * 如果case 后面跟的是一个变量，此时模式匹配语法就会将要匹配的值赋值给这个变量，从而可以在后面的处理语句中使用要匹配的值
    * */
    def judgeGrade2(name: String, grade: String) {
      val d="f"
      grade match {
        case "A" => println(name + ", you are excellent")
        case "B" => println(name + ", you are good")
        case "C" => println(name + ", you are just so so")
        // 这里需要注意一下，不论怎么样都会掉入下一个case 它会将 d 当成变量来匹配，也就是说一定会匹配上的  所以你需要 `d` 来说明它是常亮，或者用大写字母来声明
        case d => println(name + ", you are just so so,you should work harder")
        case sp if name == "leo" => println(name + ", you are a good boy, come on, your grade is " + sp)
        case sp => println("you need to work harder, your grade is " + sp)
      }
    }

  /*集合匹配示例*/
  // 对Array进行模式匹配，分别可以匹配带有指定元素的数组、带有指定个数元素的数组、以某元素打头的数组
  // 对List进行模式匹配，与Array类似，但是需要使用List特有的::操作符

  // 案例：对朋友打招呼
  def test_greeting(): Unit ={
    greeting(Array("Leo","kingcall"))
  }
  def greeting(arr: Array[String]) {
    arr match {
      case Array("Leo") => println("Hi, Leo!")//这个匹配很没有数组只能包含一个元素，就是待匹配对象
      case Array(girl1, girl2, girl3) => println("Hi, girls, nice to meet you. " + girl1 + " and " + girl2 + " and " + girl3)
      case Array("Leo", _*) => println("Hi, Leo, please introduce your friends to me.")
      case _ => println("hey, who are you?")
    }
  }

  def greeting(list: List[String]) {
    list match {
      case "Leo" :: Nil => println("Hi, Leo!")
      case girl1 :: girl2 :: girl3 :: Nil => println("Hi, girls, nice to meet you. " + girl1 + " and " + girl2 + " and " + girl3)
      case "Leo" :: tail => println("Hi, Leo, please introduce your friends to me.")
      case _ => println("hey, who are you?")
    }
  }

  /*对类型进行匹配*/
  def processException(e: Exception) {
    e match {
      case e1: IllegalArgumentException => println("you have illegal arguments! exception is: " + e1)
      case e2: FileNotFoundException => println("cannot find the file you need read or write!, exception is: " + e2)
      case e3: IOException => println("you got an error while you were doing IO operation! exception is: " + e3)
      case _: Exception => println("cannot know which exception you have!" )
    }
  }

 /*
  case class与模式匹配
  Scala中提供了一种特殊的类，用case class进行声明，中文也可以称作样例类。case class其实有点类似于Java中的JavaBean的概念。即只定义field，并且由Scala编译时自动提供getter和setter方法，但是没有method。
  case class的主构造函数接收的参数通常不需要使用var或val修饰，Scala自动就会使用val修饰（但是如果你自己使用var修饰，那么还是会按照var来）
  Scala自动为case class定义了伴生对象，也就是object，并且定义了apply()方法，该方法接收主构造函数中相同的参数，并返回case class对象
  */


  // 案例：学校门禁
  class Person
  case class Teacher(name: String, subject: String) extends Person
  case class Student(name: String, classroom: String) extends Person

  def test_judgeIdentify(): Unit ={
    judgeIdentify(new Person)
    judgeIdentify(new Teacher("kingcall","math"))
    judgeIdentify(new Student("kincall","sofa"))
  }
  def judgeIdentify(p: Person) {
    p match {
      case Teacher(name, subject) => println("Teacher, name is " + name + ", subject is " + subject)
      case Student(name, classroom) => println("Student, name is " + name + ", classroom is " + classroom)
      case _ => println("Illegal access, please go out of the school!")
    }
  }

 /*
  Option与模式匹配
  Scala有一种特殊的类型，叫做Option。Option有两种值，一种是Some，表示有值，一种是None，表示没有值。
  Option通常会用于模式匹配中，用于判断某个变量是有值还是没有值，这比null来的更加简洁明了
  option的用法必须掌握，因为Spark源码中大量地使用了Option，比如Some(a)、None这种语法，因此必须看得懂Option模式匹配，才能够读懂spark源码。
  */

  // 案例：成绩查询
  val grades = Map("Leo" -> "A", "Jack" -> "B", "Jen" -> "C")

  def getGrade(name: String) {
    val grade = grades.get(name)
    grade match {
      case Some(grade) => println("your grade is " + grade)
      case None => println("Sorry, your grade information is not in the system")
    }
  }

    /**
      * 类型匹配实例
      */


}
