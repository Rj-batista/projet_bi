import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType

object SimpleAPP extends App {
  val spark = SparkSession
    .builder()
    .appName(name = "first sparkAPP")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")


  def clean_data (a:String): DataFrame={
    val df_name= a
    val tmp_df = spark
      .read
      .option("header", true)
      .csv("src/main/data/%s" format a)
    val tmp_drop_df=tmp_df.drop("Indicator")
    tmp_drop_df.filter(tmp_drop_df("Period")==="2016")
  }

  val basicHandWashing = clean_data("basicHandWashing.csv")


}