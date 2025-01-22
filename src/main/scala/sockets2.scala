import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.sql.Timestamp 
import org.apache.spark.sql.functions.{current_timestamp, window}

object monitor2{
  def main(args: Array[String]): Unit = {
    println("Hola, Mundo!")

    // Crear la SparkSession
    val spark = SparkSession.builder()
      .appName("SocketStreamApp")
      .master("local[*]") 
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    
    import spark.implicits._

    val esquemaDF = StructType(Seq(
        StructField("transaction_id",StringType, nullable=false),
        StructField("category", StringType, nullable=false),
        StructField("amount", FloatType, nullable=false)
    ))
    

    // Entrada
    val socketDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()
  
     val jsonDF = socketDF
      .select(from_json(col("value"), esquemaDF).as("data")) 
      .select("data.*") 
      .withColumn("timestamp", current_timestamp())

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
    val ventas_total = jsonDF
      .withWatermark("timestamp", "1 minute") // Permitir retrasos de hasta 1 minuto 
      .groupBy(window($"timestamp", "10 seconds")) 
      .agg(sum("amount").alias("amount_sum")) 
      .withColumn("amount_sum", format_number(col("amount_sum"), 2))
    

    val electronicos = jsonDF
      .filter($"category" === "Electronics")

    // Salidas
    val ventas = ventas_total.writeStream
      .outputMode("complete")
      .format("console")
      .start()
      .awaitTermination() 

    // Salidas
    val electronic = electronicos.writeStream
      .outputMode("update")
      .format("console")
      .start()
      .awaitTermination()
          
  }
}