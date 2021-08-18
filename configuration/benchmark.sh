#!/bin/sh

if [ -z "${BENCHMARK_HOME}" ]; then
  export BENCHMARK_HOME="$(cd "`dirname "$0"`"; pwd)"
fi

CLIENT_DB_DATA_PATH_LINE=$(grep "DB_DATA_PATH" $BENCHMARK_HOME/conf/clientSystemInfo.properties)
CLIENT_DB_DATA_PATH=${CLIENT_DB_DATA_PATH_LINE#*=}

$BENCHMARK_HOME/bin/startup.sh -cf $BENCHMARK_HOME/conf/config.properties

#touch $CLIENT_DB_DATA_PATH/log_stop_flag