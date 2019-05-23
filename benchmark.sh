#!/bin/sh

if [ -z "${BENCHMARK_HOME}" ]; then
  export BENCHMARK_HOME="$(cd "`dirname "$0"`"; pwd)"
fi

#git pull
rm -rf lib
mvn clean package -Dmaven.test.skip=true
$BENCHMARK_HOME/bin/startup.sh -cf $BENCHMARK_HOME/conf/config.properties
