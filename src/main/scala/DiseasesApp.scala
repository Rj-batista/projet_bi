import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{avg, col, desc, split}
import org.apache.log4j.{Level, Logger}

object DiseasesAPP extends App {

  /**
   * Ouvrent un csv et formatte les données (schéma + valeurs)
   * @param filename Nom du fichier CSV à ouvrir
   * @param stat_name Nom de la colonne contenant la statistique principale
   * @param format_stat Indice de format de la statistique principale
   * @return DataFrame
   */
  def prepare_data(filename: String, stat_name: String, format_stat: Boolean): DataFrame = {

    var file = spark
      .read
      .option("header", true)
      .csv("src/main/data/%s.csv" format filename)

    file = file.drop("Indicator") // On supprime la colonne 'Indicator'

    // On définit une structure conditionnelle pour gérer le format de la statistique de la colonne 'First Tooltip'

    // 1er cas : c'est un ensemble valeur / range, on la remplace par deux colonne pour contenir ces éléments
    if(format_stat == true) {

      file = file.withColumn(stat_name, split(col("First Tooltip"), " ").getItem(0))
        .withColumn("Range_Value", split(col("First Tooltip"), " ").getItem(1))
        .drop("First Tooltip")

    }

    // 2ème cas : c'est une valeur, on modifie alors simplement le nom de la colonne
    else {
      file = file.withColumnRenamed("First Tooltip", stat_name)
    }

    file // On retourne le dataframe formaté

  }

  /**
   * Filtre les données d'un DataFrame sur la dimension principale (Sexe)
   * @param dataset DataFrame contenant les données
   * @param dimension_name Nom de la colonne contenant la dimension
   * @return /
   */
  def filter_on_dimension(dataset: DataFrame, dimension_name: String) = {

    var df_both_sexes = dataset.filter(dataset("Dim1") === "Both sexes")
      .orderBy(desc(dimension_name))
      .show()

    var df_male = dataset.filter(dataset("Dim1") === "Male")
      .orderBy(desc(dimension_name))
      .show()

    var df_female = dataset.filter(dataset("Dim1") === "Female")
      .orderBy(desc(dimension_name))
      .show()

  }

  Logger.getLogger("org").setLevel(Level.ERROR)

  System.setProperty("hadoop.home.dir", "D:\\Oussama\\ESGI\\Cours\\PROJETS\\projet_spark\\hadoop")

  val spark = SparkSession
    .builder()
    .appName(name = "first sparkAPP")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  // Fichier contenant les données liées à l'espérance de vie
  // => Espérance de vie à la naissance, par pays / date et sexe
  val life_esperance = prepare_data("lifeExpectancyAtBirth", "Life_Esperance", false)
  // life_esperance.show()

  // Fichier contenant les données liées à l'indice de mortalité à la grossesse
  // => Taux de mortalité maternelle pour 100 000 naissances par pays / date
  val maternal_mortality = prepare_data("maternalMortalityRatio", "Maternal_Mortality", true)
  // maternal_mortality.show()

  // Fichier contenant les données liées à l'indice de mortalité à la naissance
  // => Probabilité que les enfants meurent au cours des 28 premiers jours de vie par pays / date
  val neo_natal_mortality = prepare_data("neonatalMortalityRate", "Neo_Natal_Mortality", true)
  // neo_natal_mortality.show()

  /************************************************Requêtes************************************************/

  // On crée une vue pour chaque dataset à partir des DataFrame afin d'effectuer des requêtes SQL
  life_esperance.createOrReplaceTempView("life_esperance_dataset")
  maternal_mortality.createOrReplaceTempView("maternal_mortality_dataset")
  neo_natal_mortality.createOrReplaceTempView("neo_natal_mortality_dataset")

  // Requête pour obtenir l'espérence de vie minimale, maximale et moyenne par pays et par date
  val life_esperance_query_one = spark.sql("SELECT Location, Period, MIN(Life_Esperance) AS min_life_esperance, " +
    "MAX(Life_Esperance) AS max_life_esperance, AVG(Life_Esperance) AS avg_life_esperance " +
    "FROM life_esperance_dataset " +
    "GROUP BY Location, Period " +
    "ORDER BY avg_life_esperance DESC")

  // life_esperance_query_one.show()

  // Requête pour obtenir la ratio de mortalité maternelle minimale, maximale et moyenne par pays
  val maternal_mortality_query_one = spark.sql("SELECT Location, MIN(Maternal_Mortality) AS min_maternal_mortality, " +
    "MAX(Maternal_Mortality) AS max_maternal_mortality, AVG(Maternal_Mortality) AS avg_maternal_mortality " +
    "FROM maternal_mortality_dataset " +
    "GROUP BY Location " +
    "ORDER BY avg_maternal_mortality DESC")

  // maternal_mortality_query_one.show()

  // Requête pour obtenir le ratio de mortalité des nouveaux-nés minimale, maximale et moyenne par pays
  val neo_natal_mortality_query_one = spark.sql("SELECT Location, MIN(Neo_Natal_Mortality) AS min_neo_natal_mortality, " +
    "MAX(Neo_Natal_Mortality) AS max_neo_natal_mortality, AVG(Neo_Natal_Mortality) AS avg_neo_natal_mortality " +
    "FROM neo_natal_mortality_dataset " +
    "GROUP BY Location " +
    "ORDER BY avg_neo_natal_mortality DESC")

  // neo_natal_mortality_query_one.show()

  // Requête pour obtenir la moyenne des 3 statistiques de l'étude générale par pays
  val cross_join_query_one = spark.sql("SELECT A.Location, AVG(A.Life_Esperance) AS avg_life_esperance, " +
    "AVG(B.Maternal_Mortality) AS avg_maternal_mortality, AVG(C.Neo_Natal_Mortality) AS avg_neo_natal_mortality FROM " +
    "life_esperance_dataset A INNER JOIN maternal_mortality_dataset B " +
    "ON A.Location = B.Location INNER JOIN neo_natal_mortality_dataset C " +
    "ON A.Location = C.Location " +
    "GROUP BY A.Location " +
    "ORDER BY avg_life_esperance DESC")

  // cross_join_query_one.show()

  /******************************************Traitements DataFrame******************************************/

  // Traitement pour les trois catégories de la dimension Dim1 (liée au sexe) : ('Both sexes', 'Male', 'Female') pay pays et date

  // Affichage de l'espererance de vie
  //filter_on_dimension(life_esperance, "Life_Esperance")

  // Affichage du ratio de mortalité maternelle
  //filter_on_dimension(maternal_mortality, "Maternal_Mortality")

  // Affichage du ratio de mortalité infantile
  //filter_on_dimension(neo_natal_mortality, "Neo_Natal_Mortality")

  // Affichage des pays ou l'espérance de vie est supérieure à la moyenne
  //life_esperance.filter(life_esperance("Life_Esperance") > life_esperance.select(avg("Life_Esperance")).collect()(0)(0))

  // Affichage des pays ou la mortalité maternelle est supérieure à la moyenne
  //maternal_mortality.filter(maternal_mortality("Maternal_Mortality") > maternal_mortality.select(avg("Maternal_Mortality")).collect()(0)(0))
                //.show()

  // Affichage des pays ou la mortalité infantile est supérieure à la moyenne
  //neo_natal_mortality.filter(neo_natal_mortality("Neo_Natal_Mortality") > neo_natal_mortality.select(avg("Neo_Natal_Mortality")).collect()(0)(0))
                //.show()

  // Calcul des moyennes des statistiques des trois datasets
  var avg_life_esperance = life_esperance.filter(life_esperance("Life_Esperance") > life_esperance.select(avg("Life_Esperance")).collect()(0)(0))
  var avg_maternal_mortality = maternal_mortality.filter(maternal_mortality("Maternal_Mortality") > maternal_mortality.select(avg("Maternal_Mortality")).collect()(0)(0))
  var avg_neo_natal_mortality = neo_natal_mortality.filter(neo_natal_mortality("Neo_Natal_Mortality") > neo_natal_mortality.select(avg("Neo_Natal_Mortality")).collect()(0)(0))

  // Requête pour récupérer la moyenne de l'espérance de vie, de la mortalité maternelle et de la mortalité infantile par pays
  life_esperance.as("A").join(maternal_mortality.as("B"), life_esperance("Location") === maternal_mortality("Location"), "inner")
                              .join(neo_natal_mortality.as("C"), life_esperance("Location") === neo_natal_mortality("Location"), "inner")
                              .groupBy("A.Location")
                              .agg(avg("A.Life_Esperance").as("Avg_Life_Esperance"),
                                   avg("B.Maternal_Mortality").as("Avg_Maternal_Mortality"),
                                   avg("C.Neo_Natal_Mortality").as("Avg_Neo_Natal_Mortality"))
                              .orderBy(desc("Avg_Life_Esperance"))
                              .show()

}


