#FROM bde2020/spark-master:3.0.1-hadoop3.2
FROM bitnami/spark:latest

#RUN echo 'export JAVA_HOME=/usr/lib/jvm/java-1.8-openjdk/' >> /spark/bin/load-spark-env.sh
RUN echo 'export export SPARK_CLASSPATH=$SPARK_HOME/jars/*' >> conf/spark-env.sh
RUN echo 'export export HADOOP_CLASSPATH=$SPARK_HOME/jars/*' >> conf/spark-env.sh

