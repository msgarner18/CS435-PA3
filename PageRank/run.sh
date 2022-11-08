mvn package
$SPARK_HOME/bin/spark-submit target/PageRank-1.0-SNAPSHOT.jar --deploy-mode cluster --supervise ./target/PageRank-1.0-SNAPSHOT.jar