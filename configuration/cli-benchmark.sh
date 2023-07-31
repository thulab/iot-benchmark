#!/bin/sh

if [ -z "${BENCHMARK_HOME}" ]; then
  export BENCHMARK_HOME="$(cd "`dirname "$0"`"/.; pwd)"
fi

# configure the path of iotdb
IOTDB_HOME=/home/user/github/iotdb/iotdb/

#extract parameters from config.properties
DB=$(grep "^DB_SWITCH" $BENCHMARK_HOME/conf/config.properties)

echo Testing ${DB#*=} ...

# Init IoTDB
# shellcheck disable=SC2039
if [[ "${DB#*=}" =~ "IoTDB" ]]; then
  # Use default mode
  echo "initial database in server..."
  if [ "$IOTDB_HOME" ]; then
    rm -rf "$IOTDB_HOME/data";sh $IOTDB_HOME/sbin/stop-server.sh;sleep 2
  fi
  echo 'wait a few seconds for launching IoTDB...'
  sh $IOTDB_HOME/sbin/start-server.sh >/dev/null 2>&1 &
  IOTDB_PID=$!
  sleep 20
  echo IOTDB started with PID: $IOTDB_PID
fi

echo '------Client Test Begin Time------'
date
sed -i "s/server-logs/logs/g" $BENCHMARK_HOME/conf/logback.xml
sed -i "s/^BENCHMARK_WORK_MODE.*$/BENCHMARK_WORK_MODE=testWithDefaultPath/g" $BENCHMARK_HOME/conf/config.properties
./benchmark.sh
echo Stop Server Mode Benchmark ...
kill -9 $SERVER_PID
echo Stop IoTDB ...
sh $IOTDB_HOME/sbin/stop-server.sh

echo '------Client Test Complete Time------'
date