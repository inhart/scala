val scala3Version = "2.13.12"

 // Dependencias para Spark
  libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.0"            
  libraryDependencies +=  "org.apache.spark" %% "spark-sql" % "3.5.0"              
  libraryDependencies +=  "org.apache.spark" %% "spark-streaming" % "3.5.0"        
  libraryDependencies +=  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.0"  
  
 
lazy val root = project
  .in(file("."))
  .settings(
    name := "sockets",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version, 
    enablePlugins(MetalsPlugin),
    libraryDependencies += "org.scalameta" %% "munit" % "1.0.0" % Test
  )