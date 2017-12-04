#!/bin/sh

SERVER_HOST=liurui@192.168.130.1
IOTDB_HOME=/home/liurui/github/iotdb/iotdb/bin
REMOTE_BENCHMARK_HOME=/home/liurui/github/iotdb-benchmark
LOG_STOP_FLAG_PATH=/home/liurui

if [ -z "${BENCHMARK_HOME}" ]; then
  export BENCHMARK_HOME="$(cd "`dirname "$0"`"/.; pwd)"
fi

#change to client mode
sed -i 's/SERVER_MODE *= *true/SERVER_MODE=false/g' $BENCHMARK_HOME/conf/config.properties
db=$(grep "DB_SWITCH" $BENCHMARK_HOME/conf/config.properties)
querymode=$(grep "IS_QUERY_TEST" $BENCHMARK_HOME/conf/config.properties)
echo Testing ${db#*=} ...

mvn clean package -Dmaven.test.skip=true

#synchronize config server benchmark
ssh $SERVER_HOST "rm $REMOTE_BENCHMARK_HOME/conf/config.properties"
scp $BENCHMARK_HOME/conf/config.properties $SERVER_HOST:$REMOTE_BENCHMARK_HOME/conf

#start server system information recording
ssh $SERVER_HOST "sh $REMOTE_BENCHMARK_HOME/ser-benchmark.sh > /dev/null 2>&1 &"

if [ "${db#*=}" = "IoTDB" -a "${querymode#*=}" = "false" ]; then
  echo "initial database in server..."
  ssh $SERVER_HOST "cd $LOG_STOP_FLAG_PATH;rm -rf data;sh $IOTDB_HOME/stop-server.sh;sleep 2"
  ssh $SERVER_HOST "cd $LOG_STOP_FLAG_PATH;sh $IOTDB_HOME/start-server.sh > /dev/null 2>&1 &"
  echo 'wait a few seconds for lauching IoTDB...'
  sleep 15
fi

echo '------Client Test Begin Time------'
date
cd bin
sh startup.sh -cf ../conf/config.properties
wait
#stop server system information recording
ssh $SERVER_HOST "touch $LOG_STOP_FLAG_PATH/log_stop_flag"
echo '------Client Test Complete Time------'
date

