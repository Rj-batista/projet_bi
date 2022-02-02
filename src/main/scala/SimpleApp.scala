import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc

object SimpleAPP extends App {
  val spark = SparkSession
    .builder()
    .appName(name = "first sparkAPP")
    .master("local[*]")
    .getOrCreate()

  val firstdf =spark
    .read
    .option("header",true)
    .csv("src/main/data/30-70cancerChdEtc.csv")
  firstdf.show()

}