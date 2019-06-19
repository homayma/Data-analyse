import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._
import scalafx.application
import scalafx.application.JFXApp
object transport {

  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("transport").getOrCreate()

    val vehicule = spark.read.option("header", true).option("inferSchema", true).option("timestampFormat", "dd/MM/yyyy").format("csv").load("C:/spark/examples/jars/vehicules-2017.csv")
      .drop("occutc").drop("obs").drop("obsm").drop("senc").drop("catv")

    val lieux = spark.read.option("header", true).option("inferSchema", true).option("timestampFormat", "dd/MM/yyyy").format("csv").load("C:/spark/examples/jars/lieux-2017.csv")
      .drop("v1").drop("v2").drop("vosp").drop("plan").drop("surf").drop("infra").drop("situ").drop("env1")
      .drop("pr").drop("pr1").drop("prof").drop("lartpc").drop("circ").drop("nbv").drop("catr")

    import spark.implicits._

    var caracteristique = spark.read.option("header", true).option("inferSchema", true).option("timestampFormat", "dd/MM/yyyy").format("csv").load("C:/spark/examples/jars/caracteristiques-2017.csv")
      .drop("adr").drop("gps").drop("lat").drop("long").drop("dep").drop("int").drop("atm").drop("col")
    var carateristique1 = caracteristique.withColumn("mois", when(length($"mois") === 1, concat(lit("0"), col("mois"))).otherwise(col("mois")))
    var c1 = carateristique1.withColumn("mois", when(length($"mois") === 1, concat(lit("0"), col("mois"))).otherwise(col("mois")))
    var c2 = c1.withColumn(("mois"), concat(lit("-"), col("mois")))
    var c3 = c2.withColumn("an", when(col("an") === "17", "2017").otherwise(col("an")))
    var c4 = c3.withColumn("jour", when(length($"jour") === 1, concat(lit("0"), col("jour"))).otherwise(col("jour")))
    var c5 = c4.withColumn(("jour"), concat(lit("-"), col("jour")))
    var caract = c5.withColumn(("Date"), concat(col("an"), col("mois"), col("jour")))
      .drop("an").drop("mois").drop("jour")


    val usagers = spark.read.option("header", true).option("inferSchema", true).option("timestampFormat", "dd/MM/yyyy").format("csv").load("C:/spark/examples/jars/usagers-2017.csv")
      .drop(" locp").drop("actp").drop("etatp").drop("locp")

    val transport = vehicule.join(lieux, Seq("Num_Acc"), "inner").join(usagers, Seq("Num_Acc"), "inner").join(caract, Seq("Num_Acc"), "inner")
      .withColumn("lieu", lit("MONTPELLIER"))


    val M = spark.read.option("header", true).option("inferSchema", true).option("sep", ";").option("timestampFormat", "dd/MM/yyyy").format("csv").load("C:/spark/examples/jars/meteo.csv")
    val destFormat = "yyyy-MM-dd"
    val meteo = M.withColumn("Date", date_format(col("Date"), destFormat))
      .dropDuplicates("Date")
    val meteojointransport = meteo.join(transport, Seq("lieu", "Date"))

    //val Date = meteojointransport.select("Date")
    // Date.head(5)
    // meteojointransport.select(meteojointransport.col("Num_Acc")).filter($"Date"==="2017-01-11").show()

  // val k= meteojointransport.groupBy($"Date" === "2017-01-11").agg(count("Num_Acc")).show()

      }
    }
