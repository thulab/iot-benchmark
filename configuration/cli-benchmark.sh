#!/bin/sh

if [ -z "${BENCHMARK_HOME}" ]; then
  export BENCHMARK_HOME="$(cd "`dirname "$0"`"/.; pwd)"
fi

# configure the related path and host name
IOTDB_HOME=/home/user/github/iotdb/iotdb/

#extract parameters from config.properties
DB=$(grep "^DB_SWITCH" $BENCHMARK_HOME/conf/config.properties)

echo Testing ${DB#*=} ...

# Init ServerMode
echo Start server system information recording ...
nohup $BENCHMARK_HOME/ser-benchmark.sh > /dev/null 2>&1 &
SERVER_PID=$!
echo ServerMode started with PID: $SERVER_PID

# Init IoTDB
# shellcheck disable=SC2039
if [[ "${DB#*=}" =~ "IoTDB" ]]; then
  # Use default mode
  sed -i "s/^BENCHMARK_WORK_MODE.*$/BENCHMARK_WORK_MODE=testWithDefaultPath/g" $BENCHMARK_HOME/conf/config.propertie
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
./benchmark.sh
echo Stop Server Mode Benchmark ...
kill $SERVER_PID
echo Stop IoTDB ...
sh $IOTDB_HOME/sbin/stop-server.sh

echo '------Client Test Complete Time------'
date