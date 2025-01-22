import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.sql.Timestamp 
import org.apache.spark.sql.functions.{current_timestamp, window}

case class Esquema(timestamp:String, cpu_usage: Int, memory_usage: Int, disk_usage: Int)

object monitor1 {
  def main(args: Array[String]): Unit = {
    println("Hola, Mundo!")

    // Crear la SparkSession
    val spark = SparkSession.builder()
      .appName("SocketStreamApp")
      .master("local[*]") 
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    
    import spark.implicits._

    // Entrada
    val socketDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()
  
    // Convertir los datos a un DataFrame estructurado con el esquema
    val esquemaDF = socketDF
       .withColumn("timestamp", split($"value", "\\|").getItem(0)) 
       .withColumn("cpu_usage", split($"value", "\\|").getItem(1)) 
       .withColumn("memory_usage", split($"value", "\\|").getItem(2)) 
       .withColumn("disk_usage", split($"value", "\\|").getItem(3)) 
       .withColumn("timestamp", (col("timestamp").cast("long") * 1000).cast(TimestampType))
   
    /* OTRA FORMA CON maps  
    val esquemaDF = socketDF
        .as[String]
        .filter(_.nonEmpty)        
        .map(line => {
            val parts = line.split("\\|")
            Esquema(parts(0), parts(1).toInt, parts(2).toInt, parts(3).toInt)
        })
        .toDF()
        .withColumn("timestamp", (col("timestamp").cast("long") * 1000).cast(TimestampType)) 
     */
            
    //1. Calcular la media de cpu_usage con vistas 
    //NO SE PUEDE USAR VENTANAS DE TIEMPO DIRECTAMENTE
    //esquemaDF.createOrReplaceTempView("monitor")
    //val mediaCPU = spark.sql("SELECT AVG(cpu_usage) AS media_cpu_usage FROM monitor")
    
    //2. Calcular la media de cpu_usage con dataframes. Devuelve un unico valor cpu_usage.
    //val mediaCPU = esquemaDF.select(avg("cpu_usage"))
   
    //3. Calcular la media de cpu_usage con dataframes y ventana de tiempo
    // Devuelve un dataframe (timestamp y la media)
    val mediaCPU = esquemaDF
      .withWatermark("timestamp", "1 minute") // Permitir retrasos de hasta 1 minuto 
      .groupBy(window($"timestamp", "10 seconds")) 
      .agg(avg("cpu_usage").alias("cpu_usage_avg")) 
    
    val alertaCPU = esquemaDF
      .filter($"disk_usage" > 80)


    // Salidas
    val media = mediaCPU.writeStream
      .outputMode("complete")
      .format("console")
      .start()

      
    // Salidas
    val alerta = alertaCPU.writeStream
      .outputMode("update")
      .format("console")
      .start()

    media.awaitTermination()  
    alerta.awaitTermination()  
  }
}
