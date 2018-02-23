package scala.AkkaDM.wordcount

import scala.collection.mutable.{ArrayBuffer, HashMap}

/**
  * Created by Dante on 2017/6/18.
  */

class Word(val word:String,val count:Integer)
case class Result()
class MapData(val dataList: ArrayBuffer[Word])
class ReduceData(val reduceDataMap: HashMap[String, Integer])
