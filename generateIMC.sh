$HADOOP_HOME/bin/hdfs dfs -rm -r output
$HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main MapReduce.java
jar cf wc.jar MapReduce*.class
$HADOOP_HOME/bin/hadoop jar wc.jar MapReduce socio/socio_1000.csv output
$HADOOP_HOME/bin/hadoop fs -cat output/part-r-00000 > outputIMC.txt