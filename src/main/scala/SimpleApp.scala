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
    val size_df = tmp_df.columns.size
    size_df match{
      case 6 =>
      tmp_df
        .filter(tmp_df("Indicator")==="Ambient and household air pollution attributable death rate (per 100 000 population, age-standardized)")
        .withColumn("Full_value",split(col("First Tooltip")," ").getItem(0))
        .withColumn("Range_value",split(col("First Tooltip")," ").getItem(1))
        .drop("First Tooltip")
      case _ =>
        val tmp_df_year=tmp_df
          .filter(tmp_df("Period")==="2016")
          .drop("Indicator")
          .withColumn("First Tooltip",col("First Tooltip").cast(FloatType))
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

  /**
   * Cette partie du code comprend l'ensemble des reqûetes sur les différentes dataframes
   * Le but est d'extraire un ensemble de résultats sur la situation sanitaire/environnementale des pays du monde
   *
   *
   */

  //---------------------------------------------------------------------------------------------------
  // Le % de population avec l'accès à l'eau potable basique (Search engine)
  //---------------------------------------------------------------------------------------------------
  def drink_water(country:String,annee:String):DataFrame={
    val basic_drink = spark
      .read
      .option("header", true)
      .csv("src/main/data/basicDrinkingWaterServices.csv")
      .withColumn("First Tooltip",col("First Tooltip").cast(FloatType))
      .withColumnRenamed("First Tooltip","% least basic service" )
      .drop("Indicator")
    basic_drink
      .filter(basic_drink("Location")===country && basic_drink("Period")===annee)
  }
  //drink_water("Albania","2003").show()

    //---------------------------------------------------------------------------------------------------
    // Le % de population avec des moyens de nettoyage des mains basiques à la maison en 2016
    //---------------------------------------------------------------------------------------------------
  val basic_hand  = clean_data("basicHandWashing.csv","% basic handwashing")
  basic_hand
    .filter(basic_hand("Dim1")==="Total") // Les deux
    .orderBy(desc("% basic handwashing"))
    //.show()
  basic_hand
    .filter(basic_hand("Dim1")==="Rural")// En zone rural
    .orderBy(asc("% basic handwashing"))
    //.show()
  basic_hand
    .filter(basic_hand("Dim1")==="Urban")// En zone ubraine
    .orderBy(desc("% basic handwashing"))
    //.show()

  //---------------------------------------------------------------------------------------------------
  // Pour 100k habitant les morts attribués à la pollution de l'air,age-standardized (SQL Queries)
  //---------------------------------------------------------------------------------------------------

  val air_pollution= clean_data("airPollutionDeathRate.csv")
  air_pollution.createOrReplaceTempView("air_pollution")
  // Nombre de mort pour 100k d'habitant supérieur à 250
  //spark.sql("SELECT * FROM air_pollution WHERE air_pollution.Full_value > 250").show()

  // Nombre de mort maximum les deux sexes & Nombre de mort maximum Homme ou Femme
    //spark.sql("SELECT Location, Dim2,MAX(Full_value) FROM air_pollution " +
      //                "WHERE air_pollution.Dim1 = 'Both sexes' GROUP BY Location,Dim2").show()
    //spark.sql("SELECT Location,Dim2,MAX(Full_value) FROM air_pollution " +
      //                "WHERE air_pollution.Dim1 = 'Female' GROUP BY Location,Dim2").show()

  //Nombre de mort total par groupe de maladie (Ici pulmonaire)

  //spark.sql("SELECT Location,SUM(Full_value) AS Pulmonary_Diseases " +
                      "FROM air_pollution " +
                      "WHERE air_pollution.Dim2='Trachea, bronchus, lung cancers' " +
                      "OR air_pollution.Dim2='Lower respiratory infections' " +
                      "OR air_pollution.Dim2='Chronic obstructive pulmonary disease'" +
 //                   "GROUP BY Location").show()

  //spark.sql("SELECT Location,SUM(Full_value) AS Heart_Diseases " +
                    "FROM air_pollution " +
                    "WHERE air_pollution.Dim2='Ischaemic heart disease' " +
                    "OR air_pollution.Dim2='Stroke' " +
 //                 "GROUP BY Location").show()

  //---------------------------------------------------------------------------------------------------
  // Nombre de mort pour 100k habitant d'empoisonnement involontaire ou d'un système d'hygiène infecté
  //---------------------------------------------------------------------------------------------------





}

