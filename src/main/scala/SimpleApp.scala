import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, StructField, StructType}



object SimpleAPP extends App {
  val spark = SparkSession
    .builder()
    .appName(name = "first sparkAPP")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  /**
   * clean_data
   *
   *  @return DataFrame
   */
  def clean_data (str:String,str_2:String=""): DataFrame={
    val tmp_df = spark
      .read
      .option("header", true)
      .csv("src/main/data/%s" format str)
      .withColumn("First Tooltip",col("First Tooltip").cast(FloatType))
    val size_df = tmp_df.columns.size
    size_df match{
      case 6 =>
        tmp_df
          .filter(tmp_df("Indicator")==="Ambient and household air pollution attributable death rate (per 100 000 population, age-standardized)")
          .filter(tmp_df("Dim1")==="Male"||tmp_df("Dim1")==="Female")
      case _ =>
        val tmp_df_year=tmp_df
          .filter(tmp_df("Period")==="2016")
          .drop("Indicator")
        tmp_df_year.withColumnRenamed("First Tooltip","%s" format str_2)

    }

  }
  /**
   * merge_df
   *
   *  @return DataFrame
   */
  def merge_df ():DataFrame={
    val tmp_1= clean_data("mortalityRateUnsafeWash.csv","Mortality UnsafeW 100K")
      .sort("Location") //Load first csv

    val tmp_2= clean_data("mortalityRatePoisoning.csv","Mortality Poisoning 100K")
      .sort("Location")
      .withColumn("rowId1",monotonically_increasing_id()) //Load second csv and add ID column

    val tmp_3=tmp_1.select("Mortality UnsafeW 100K")
      .withColumn("rowId2",monotonically_increasing_id()) //Extract wanted columns from dataset

    tmp_2.as("tmp_2")
      .join(tmp_3.as("tmp_3"),tmp_2("rowId1")===tmp_3("rowId2"),"inner")
      .select("tmp_2.Location","tmp_2.Period","tmp_2.Dim1",
                    "tmp_2.Mortality Poisoning 100K","tmp_3.Mortality UnsafeW 100K") //Join two data

  }

  // Requête sur les dataset
  val basic_hand  = clean_data("basicHandWashing.csv","% basic handwashing")
  basic_hand
    .filter(basic_hand("Dim1")==="Total")
    .orderBy(desc("% basic handwashing"))
    .show()

  val basic_drink = clean_data("basicDrinkingWaterServices.csv","% least basic service")







}

