
import org.apache.spark.sql.types.{StringType, StructField}
import org.apache.spark.sql.{Row, SparkSession}

object Expense {
  def main(args:Array[String]): Unit ={
    println("Test");
    val location = if(args.length == 0 )"C:\\Users\\kathi\\Desktop\\shivaji_expenses.csv" else args(0);

    val context = SparkSession.builder().appName("Expense").getOrCreate().sparkContext;
    val source = context.textFile(location).map(_.split(","));
    val array = Array(StructField("date", StringType, nullable = false ))
    println(source);

}}
