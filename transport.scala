import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._

object transport {

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("transport").getOrCreate()
    val t1 = spark.read.option("header", true).option("inferSchema", true).option("timestampFormat", "dd/MM/yyyy").format("csv").load("hdfs:///tmp/transport/vehicules-2017.csv")
    val t2 = spark.read.option("header", true).option("inferSchema", true).option("timestampFormat", "dd/MM/yyyy").format("csv").load("hdfs:///tmp/transport/lieux-2017.csv")
    var t3 = spark.read.option("header", true).option("inferSchema", true).option("timestampFormat", "dd/MM/yyyy").format("csv").load("hdfs:///tmp/transport/caracteristiques-2017.csv")
    val t4 = spark.read.option("header", true).option("inferSchema", true).option("timestampFormat", "dd/MM/yyyy").format("csv").load("hdfs:///tmp/transport/usagers-2017.csv")
    val t6 = spark.read.option("header", true).option("inferSchema", true).option("sep",";").option("timestampFormat", "dd/MM/yyyy").format("csv").load("hdfs:///tmp/DATA/WEATHER/Meteo.csv")
    val destFormat = "yyyy-MM-dd"
    val t62 = t6.withColumn("date",date_format(col("Date"),destFormat))
   // t62.show()
    import spark.implicits._
    val t8=t3.withColumn("mois",when(length($"mois") === 1,concat(lit("0"), col("mois"))).otherwise(col("mois")))
    val DD=t8.withColumn(("mois"),concat(lit("-"),col("mois")))
    val newdf = DD.withColumn("an", when(col("an") === "17", "2017").otherwise(col("an")))
    val tt=newdf.withColumn("jour",when(length($"jour") === 1,concat(lit("0"), col("jour"))).otherwise(col("jour")))
    val ll=tt.withColumn(("jour"),concat(lit("-"),col("jour")))

    val f= ll.withColumn(("Date"),concat(col("an"),col("mois"),col("jour") ))
    val t5= f.drop("an").drop("mois").drop("jour")
    val df = t1.join(t2, Seq("Num_Acc"), "inner").join(t5,Seq("Num_Acc"), "inner").join(t4,Seq("Num_Acc"), "inner")
//   df.select(df("Date")).show()
  //df.select(df.col("Num_Acc")).where(df.col("Date")==="2017-01-11").show()
    val g=df.join(t62,"Date")
  val d=g.filter($"Date"==="2017-06-27").select($"*",$"sexe".alias("S"))
d.show()

  }}

