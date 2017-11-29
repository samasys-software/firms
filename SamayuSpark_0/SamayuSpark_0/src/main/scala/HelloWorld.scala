import java.sql.Timestamp
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._

object HelloWorld {

  def main(args:Array[String]): Unit ={
    println("Test");
    val spark = SparkSession.builder().appName("Test").master("local").getOrCreate();
    val context = SparkSession.builder().appName("Test").master("local").getOrCreate().sparkContext;
    import org.apache.spark.sql.types._
    val timestampType = new Timestamp(System.currentTimeMillis());
    val array = Array(StructField("Ticker", StringType, nullable = true), StructField("Date", StringType, nullable = true),
                      StructField("Price", DoubleType, nullable = true), StructField("updated_time", TimestampType, nullable = false));
    val schema = StructType( array);
    val source = context.textFile(args(0)).map(_.split(","))
    val br = context.broadcast(timestampType);
    val sourceFile = source.map(x=> Row( "tcs", x(0), x(5).toDouble, br.value));
    val dr= spark.createDataFrame(sourceFile, schema);
    dr.createOrReplaceTempView("apple_quotes")
    dr.show();
    spark.sql(args(1)).foreach(row=> (println(row(0), row(1), row(2), row(3))));

  }
}
      