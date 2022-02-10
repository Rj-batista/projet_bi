import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.FloatType


object DiseasesApp extends App {
  val spark = SparkSession
    .builder()
    .appName(name = "first sparkAPP")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")


  // Dans un premier temps,créons les dataframes avec les données qui nous intéressent
  // et regardons ce que l'on obtient
   */
  val malaria = spark
    .read
    .option("inferSchema","true")
    .option("header","true")
    .csv("src/main/data/incedenceOfMalaria.csv")
  malaria.printSchema()
  malaria.show(numRows = 20)

  val tuberculosis = spark
  .read
    .option("inferSchema","true")
    .option("header","true")
    .csv("src/main/data/incedenceOfTuberculosis.csv")
  tuberculosis.printSchema()
  tuberculosis.show(numRows = 20)

  val HIV = spark
    .read
    .option("inferSchema","true")
    .option("header","true")
    .csv("src/main/data/newHivInfections.csv")
  HIV.printSchema()
  HIV.show(numRows = 20)

  val hepatitusB = spark
    .read
    .option("inferSchema","true")
    .option("header","true")
    .csv("src/main/data/hepatitusBsurfaceAntigen.csv")
  hepatitusB.printSchema()
  hepatitusB.show(numRows = 20)

  val NTD = spark
    .read
    .option("inferSchema","true")
    .option("header","true")
    .csv("src/main/data/interventionAgianstNTDs.csv")
  NTD.printSchema()
  NTD.show(numRows = 20)

  val cancer = spark
    .read
    .option("inferSchema","true")
    .option("header","true")
    .csv("src/main/data/30-70cancerChdEtc.csv")
  cancer.printSchema()
  cancer.show(numRows = 20)

  val suicides = spark
    .read
    .option("inferSchema","true")
    .option("header","true")
    .csv("src/main/data/crudeSuicideRates.csv")
  suicides.printSchema()
  suicides.show(numRows = 20)

// Passsons maintenant à la phase de transformation de ces dataframes
// pour les rendre exploitables et faire des requêtes dessus


// Nous pouvons maintenant faire nos requetes sql et sortir des statistiques


