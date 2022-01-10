#!/bin/bash
# java/spark/build.sh
cd "$(dirname "$0")"/ || exit
mvn --version
mvn clean package -Dmaven.test.skip=true

cd target
mkdir spark-cluster-shell
cp ../script/spark-cluster-shell.sh spark-cluster-shell
cp spark-cluster-shell.jar spark-cluster-shell
tar zcvf spark-cluster-shell.tar.gz spark-cluster-shell
