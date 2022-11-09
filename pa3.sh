#go to directory
cd ~/cs435/PA3/PageRankScala
#remove old data
rm ./target/scala-2.12/pagerankscala_2.12-0.1.jar
$HADOOP_HOME/bin/hadoop fs -rm -r /PA3
#compile jar
sbt clean compile package
#put data in HDFS
$HADOOP_HOME/bin/hadoop fs -put ./ /PA3
#run jar file
$SPARK_HOME/bin/spark-submit --class PageRank --deploy-mode cluster --supervise ./target/scala-2.12/pagerankscala_2.12-0.1.jar /PA3/input/small-links.txt /PA3/input/small-titles.txt /PA3/output/
#view output
# $HADOOP_HOME/bin/hadoop fs -cat /PA3/output/part-00000
# $HADOOP_HOME/bin/hadoop fs -cat /PA3/output/part-00001