# CS435-PA3
- Apparently, the apache spark tar file is considered to be too big to be saved on GitHub, so it needs to be added in the .gitignore file
- I already did this, so you should probably be fine

## Apache Spark Setup Tutorial I ended up following
- https://www.youtube.com/watch?v=uUawCJY86Hk

### Downloading Apache Spark
- I went to https://spark.apache.org/downloads.html and downloaded spark-3.3.1-bin-hadoop3.tgz
- at CS435-PA3, I unpacked spark-3.3.1-bin-hadoop3.tgz to create spark-3.3.1-bin-hadoop3

### Setting up Apache Spark
- I went to spark-3.3.1-bin-hadoop3/conf and ran  cp spark-env.sh.template spark-env.sh
- copied export "JAVA_HOME=..." line from ~/.bashrc and pasted it in spark-env.sh after the following comments:

This file is sourced when running various Spark programs.

Copy it as spark-env.sh and edit that to configure Spark for your site.

## Maven
- this is what I am using to create/run our Apache Spark code

### Setup
In ~/.bashrc, add the following 2 lines to the end of your file and adjust to where you have placed your spark-3.3.1-bin-hadoop3/ folder:
- export SPARK_HOME=~/CS435/CS435-PA3/spark-3.3.1-bin-hadoop3/
- export PATH=$PATH:$PATH_HOME/bin

### Changing the code
- the main scala code is at PageRank/src/main/scala/com/mycompany/App.scala

### Run
In PageRank, run the following:

./run.sh

You may need to run chmod +x on run.sh beforehand

## Extra Potentially useful commands
### Create Skeleton for PageRank app
In CS435-PA3, run the following:
- mvn archetype:generate -DarchetypeGroupId=net.alchim31.maven -DarchetypeArtifactId=scala-archetype-simple

groupId: com.mycompany
artifactId: PageRank
Press enter for everything else and it will give default values

### Compiling PageRank app
In CS435-PA3/PageRank, run the following:
- mvn compile

### Compile Test Sources and run unit tests
In CS435-PA3/PageRank, run the following:
- mvn test

If you only want to compile your test sources (but not execute them), run the following:
-mvn test-compile

### Create jar
In CS435-PA3/PageRank, run the following:
-mvn package

When the jar file is created, it will be found under PageRank/target
