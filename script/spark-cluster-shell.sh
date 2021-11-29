#!/bin/bash

#host=$(ip route get 1 | awk '{print $NF;exit}')
port=9999

jars="$(dirname "$0")/spark-cluster-shell.jar"
#queue="dev_test_audience"
#export SPARK_SUBMIT_OPTS="$SPARK_SUBMIT_OPTS "
#  --conf "spark.driver.extraJavaOptions=-Xdebug -Xrunjdwp:transport=dt_socket,suspend=n,server=y,address=18099" \
#  --queue "$queue" \
spark-submit \
  --master local \
  --class sucicada.susparkshell.SuSparkShellDriver \
  $jars \
  --name "Spark_Cluster_Shell" \
  --master yarn \
  --deploy-mode cluster \
  --su.spark-submit.mode SHELL \
  --su.driver.port $port \
  $*
