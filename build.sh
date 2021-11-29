#!/bin/bash
# java/spark/build.sh
cd "$(dirname "$0")"/ || exit
mvn --version
mvn clean package -Dmaven.test.skip=true
