#!/bin/sh

SPARK="spark-3.2.0-bin-hadoop3.2"
SPARK_ARCHIVE="$SPARK.tgz"
SPARK_DOWNLOAD="https://archive.apache.org/dist/spark/spark-3.2.0/$SPARK_ARCHIVE"

# Download and extract Spark
wget $SPARK_DOWNLOAD
tar -xvf $SPARK_ARCHIVE

AWS_HADOOP_JAR="https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar"
AWS_SDK_JAR="https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar"

# Download prerequisite JAR files for S3
current=$(pwd)
cd "./$SPARK/jars"
wget $AWS_HADOOP_JAR
wget $AWS_SDK_JAR
cd $current

# Build Spark
./$SPARK/bin/docker-image-tool.sh -t latest -p ./$SPARK/kubernetes/dockerfiles/spark/bindings/python/Dockerfile build
