#!/bin/sh

if [ -z "${BENCHMARK_HOME}" ]; then
  export BENCHMARK_HOME="$(cd "`dirname "$0"`"; pwd)"
fi

CLIENT_LOG_STOP_FLAG_PATH_LINE=$(grep "LOG_STOP_FLAG_PATH" $BENCHMARK_HOME/conf/clientSystemInfo.properties)
CLIENT_LOG_STOP_FLAG_PATH=${CLIENT_LOG_STOP_FLAG_PATH_LINE#*=}

#git pull
rm -rf lib
mvn clean package -Dmaven.test.skip=true
$BENCHMARK_HOME/bin/startup.sh -cf $BENCHMARK_HOME/conf/config.properties

touch $CLIENT_LOG_STOP_FLAG_PATH/log_stop_flag