package espias

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.window

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
      .orderBy($"count".desc)

    //val prohibidas = conteoPalabrasMasUsadas.limit(10)

    println("Preparando escritor a consola...")
    val query = conteoPalabrasSlide.writeStream
      .outputMode("complete") // uso 'complete' porque no me dejó usar 'update' con este modo
      .format("console")
      // .option("checkpointLocation", "checkpoint") // quito el checkpoint porque sino no puedo vivir borrando la carpeta todo el tiempo
      .option("truncate", false) // para que se vean los campos completos
      .start()

    println("Listo!")

    query.awaitTermination()

    /*

    Hola Ricardo,

    Haciendo bin/kafka-console-producer.sh --broker-list localhost:9092 --topic Celebram < MensajesCapturados.csv

    he llegado a obtener esto:

Cargando lista negra...
Lista negra cargada.
Abriendo stream de kafka (Celebram)...
Kafka Stream 'Celebram' abierto.
Configurando cargador de palabras a DataFrame...
Agrupador de palabras por frecuencia...
Preparando escritor a consola...
Listo!
-------------------------------------------
Batch: 2
-------------------------------------------
+------------------------------------------+-------------+-----+
|window                                    |palabra      |count|
+------------------------------------------+-------------+-----+
|[2019-10-26 20:42:20, 2019-10-26 20:42:30]|e-business   |38   |
|[2019-10-26 20:42:25, 2019-10-26 20:42:35]|e-business   |38   |
|[2019-10-26 20:42:20, 2019-10-26 20:42:30]|relationships|33   |
|[2019-10-26 20:42:25, 2019-10-26 20:42:35]|relationships|33   |
|[2019-10-26 20:42:25, 2019-10-26 20:42:35]|users        |31   |
|[2019-10-26 20:42:20, 2019-10-26 20:42:30]|users        |31   |
|[2019-10-26 20:42:20, 2019-10-26 20:42:30]|e-services   |30   |
|[2019-10-26 20:42:25, 2019-10-26 20:42:35]|e-services   |30   |
|[2019-10-26 20:42:25, 2019-10-26 20:42:35]|supply-chains|30   |
|[2019-10-26 20:42:20, 2019-10-26 20:42:30]|supply-chains|30   |
|[2019-10-26 20:42:25, 2019-10-26 20:42:35]|real-time    |29   |
|[2019-10-26 20:42:20, 2019-10-26 20:42:30]|real-time    |29   |
|[2019-10-26 20:42:25, 2019-10-26 20:42:35]|action-items |28   |
|[2019-10-26 20:42:20, 2019-10-26 20:42:30]|action-items |28   |
|[2019-10-26 20:42:20, 2019-10-26 20:42:30]|web-readiness|27   |
|[2019-10-26 20:42:25, 2019-10-26 20:42:35]|web-readiness|27   |
|[2019-10-26 20:42:20, 2019-10-26 20:42:30]|initiatives  |27   |
|[2019-10-26 20:42:25, 2019-10-26 20:42:35]|initiatives  |27   |
|[2019-10-26 20:42:25, 2019-10-26 20:42:35]|content      |26   |
|[2019-10-26 20:42:20, 2019-10-26 20:42:30]|e-tailers    |26   |
+------------------------------------------+-------------+-----+
only showing top 20 rows

Llevo dias tratando de hacer un limit de los resultados sin éxito, todas las formas que veo en
stackoverflow terminan sin compilar porque dice que no se puede hacer limit con streaming.

Tampoco he conseguido escribir los resultados a disco.
Lo más cerca que he estado es con esto: https://stackoverflow.com/questions/49026429/how-to-use-update-output-mode-with-fileformat-format
y lo que queda en el disco no se como procesarlo.

*/

  }
}
