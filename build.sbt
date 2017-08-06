name := "TwitterToElasticsearch"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.6.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "1.6.0"
libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark" % "2.1.0.Beta4"