$HADOOP_HOME/bin/hdfs dfs -rm -r output
$HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main WordCount.java
jar cf wc.jar WordCount*.class
$HADOOP_HOME/bin/hadoop jar wc.jar WordCount wp_20 output
$HADOOP_HOME/bin/hadoop fs -cat output/part-r-00000 > output.txt