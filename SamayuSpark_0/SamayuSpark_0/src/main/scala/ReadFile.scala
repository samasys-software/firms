import org.apache.spark.sql.SparkSession

object ReadFile {

  def main(args: Array[String]): Unit = {
    val location = args(0);
    val sc = SparkSession.builder().appName("Read").getOrCreate();
    val iterator = scala.io.Source.fromFile(location).getLines();
    val source = iterator.drop(1).toSeq;
    val rdd = sc.sparkContext.parallelize(source);
    println(rdd.map(_.split(",")).count());
  }

}
