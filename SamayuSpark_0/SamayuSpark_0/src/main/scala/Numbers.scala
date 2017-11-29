object Numbers {
  def entryValue(result : Tuple2[Int, Int], entry : Int) = {
    (result._1 + entry , result._2+1)
  }
  def tupleFunc(left : Tuple2[Int, Int], right: Tuple2[Int, Int]) = {
    (left._1 + right._1 , left._2 + right._2)
  }
  def main(args:Array[String]): Unit = {
    val result = (1 to 10).aggregate((0,0))(entryValue, tupleFunc)
    val avg    = result._1 / result._2
    println(avg)
  }
}
