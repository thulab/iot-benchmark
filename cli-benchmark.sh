#!/bin/sh

if [ -z "${BENCHMARK_HOME}" ]; then

  export BENCHMARK_HOME="$(cd "`dirname "$0"`"/.; pwd)"

fi

#change to client mode
sed -i 's/SERVER_MODE *= *true/SERVER_MODE=false/g' $BENCHMARK_HOME/conf/config.properties

mvn clean package -Dmaven.test.skip=true

#initial database in server
IOTDB_HOME=/home/liurui/github/iotdb/iotdb/bin
SERVER_HOST=liurui@192.168.130.9
REMOTE_BENCHMARK_HOME=/home/liurui/github/iotdb-benchmark
LOG_STOP_FLAG_PATH=/home

ssh $SERVER_HOST "rm -rf data;sh $IOTDB_HOME/stop-server.sh;sleep 2"
ssh $SERVER_HOST "sh $IOTDB_HOME/start-server.sh > /dev/null 2>&1 &"

echo 'wait a few seconds for lauching IoTDB...'

ssh $SERVER_HOST "sh $REMOTE_BENCHMARK_HOME/ser-benchmark.sh > /dev/null 2>&1 &"
sleep 10

echo '------Client Test Begin Time------'
date
cd bin
sh startup.sh -cf ../conf/config.properties
wait
ssh $SERVER_HOST "touch LOG_STOP_FLAG_PATH/log_stop_flag"
echo '------Client Test Complete Time------'
date

