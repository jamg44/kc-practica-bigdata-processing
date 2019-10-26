package espias

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.types.StructType

import scala.io.Source

object EspiarMensajes {

  def topic = "Celebram"

  def main(args: Array[String]): Unit = {

    // quitamos mensajes de log que no nos interesan
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("Espiar Mensajes")
      .master("local[*]") // Trabajar tantos cores como tenga disponibles
      .getOrCreate()

    /**
     * Cargar lista negra
     */
    println("Cargando lista negra...")
    val palabrasListaNegra = Source.fromFile("data/ListaNegra.txt").getLines.toList
    println("Lista negra cargada.")
    //palabrasListaNegra.foreach(println)


    /**
     * Cargar usuarios
     */
    // crear una estructura en cuanto a tipos (esquema)
    /*val esquemaRegistro = new StructType()
      .add("sample", "long")
      .add("cThick", "integer")
      .add("uCSize", "integer")
      .add("uCShape", "integer")
      .add("mAdhes", "integer")

    // crear un dataframe leyendo un CSV pasándole el esquema creado
    val dataSamplesDF = spark.read.format("csv")
      .option("header", false)
      .schema(esquemaRegistro)
      .load("data/breast-cancer-wisconsin.txt")

    // para ver lo que he cargado
    //dataSamplesDF.show()
    dataSamplesDF.createOrReplaceTempView("cancerTable")

    val dataSamplesSQL = spark.sql("SELECT sample, uCSize from cancerTable")

    dataSamplesSQL.show()*/


    /**
     * Procesar mensajes
     */
    import spark.implicits._

    println(s"Abriendo stream de kafka (${topic})...")
    val data = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", topic) // si hay más de uno se separan con comas
      .option("includeTimeStamp", true) //
      .load()
      .selectExpr("CAST(value AS STRING)", "timestamp") // value llega en binario, lo pasamos a STRING
      .as[(String, Timestamp)] // lo sacamos como una Tupla
    println(s"Kafka Stream '${topic}' abierto.")

    def partePalabrasYTimestamp(par: (String, Timestamp)): Array[(String, Timestamp)] = {
      val palabras = par._1.split(",")(1)
      val timestamp = par._2
      palabras.split(" ").map(palabra => (palabra, timestamp))
    }

    // proceso
    println("Configurando cargador de palabras a DataFrame...")
    val palabras = data.flatMap(partePalabrasYTimestamp)
      .toDF("palabra", "timestamp")

    // ventana de tiempo
    // slide: 5 seconds
    println("Agrupador de palabras por frecuencia...")
    val conteoPalabrasSlide = palabras
      .groupBy(
        window($"timestamp", "10 seconds", "5 seconds"),
        $"palabra"
      ).count()
      //.orderBy("window")


    val conteoPalabrasMasUsadas = conteoPalabrasSlide
      .orderBy($"count".desc)

    /*val df = conteoPalabrasMasUsadas.toDF()
    df.createOrReplaceTempView("masusadas")
    val masUsadas = spark.sql("select * from masusadas")
    masUsadas.show()*/

    //val prohibidas = conteoPalabrasMasUsadas.limit(10)

    println("Preparando escritor a consola...")
    val query = conteoPalabrasMasUsadas.writeStream
      .outputMode("complete") // uso 'complete' porque no me dejó usar 'update' con este modo
      .format("console")
      // .option("checkpointLocation", "checkpoint") // quito el checkpoint porque sino no puedo vivir borrando la carpeta todo el tiempo
      .option("truncate", false) // para que se vean los campos completos
      .start()
    println("Listo!")

    query.awaitTermination()

  }
}
