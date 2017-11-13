#!/bin/sh

if [ -z "${BENCHMARK_HOME}" ]; then

  export BENCHMARK_HOME="$(cd "`dirname "$0"`"/.; pwd)"

fi

#change to client mode
sed -i 's/SERVER_MODE *= *true/SERVER_MODE=false/g' $BENCHMARK_HOME/conf/config.properties

mvn clean package -Dmaven.test.skip=true

#initial database in server
IOTDB_HOME=/home/hadoop/xuyi/iotdb/iotdb/bin
ssh hadoop@192.168.130.16 "rm -rf data;sh $IOTDB_HOME/stop-server.sh;sleep 2"
ssh hadoop@192.168.130.16 "sh $IOTDB_HOME/start-server.sh > /dev/null 2>&1 &"

echo 'wait 15s for lauching IoTDB...'
sleep 15

ssh hadoop@192.168.130.16 "sh /home/hadoop/liurui/github/iotdb-benchmark/ser-benchmark.sh > /dev/null 2>&1 &"
echo '------Client Test Begin Time------'
date
cd bin
sh startup.sh -cf ../conf/config.properties
wait
ssh hadoop@192.168.130.16 "touch /home/hadoop/liurui/log_stop_flag"
echo '------Client Test Complete Time------'
date

