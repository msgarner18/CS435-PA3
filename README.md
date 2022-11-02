# CS435-PA3
Apparently, the apache spark tar file is considered to be too big to be saved on GitHub, so it needs to be added in the .gitignore file

## Apache Spark Setup Tutorial I ended up following
- https://www.youtube.com/watch?v=uUawCJY86Hk

### Downloading Apache Spark
- I went to https://spark.apache.org/downloads.html and downloaded spark-3.3.1-bin-hadoop3.tgz
- at CS435-PA3, I unpacked spark-3.3.1-bin-hadoop3.tgz to create spark-3.3.1-bin-hadoop3

### Setting up Apache Spark
- I went to spark-3.3.1-bin-hadoop3/conf and ran cp spark-env.sh.template spark-env.sh
- copied export "JAVA_HOME=..." line from ~/.bashrc and pasted it in spark-env.sh after the following comments:

This file is sourced when running various Spark programs.

Copy it as spark-env.sh and edit that to configure Spark for your site.

### Running Apache Spark
- sbin/start-all.sh starts cluster
- sbin/stop-all.sh stops cluster
- jps shows if this was successful
