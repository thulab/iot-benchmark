#!/bin/sh

if [ -z "${BENCHMARK_HOME}" ]; then

  export BENCHMARK_HOME="$(cd "`dirname "$0"`"/.; pwd)"

fi

#change to client mode
sed -i 's/SERVER_MODE *= *true/SERVER_MODE=false/g' $BENCHMARK_HOME/conf/config.properties

mvn clean package -Dmaven.test.skip=true

ssh hadoop@192.168.130.16 "rm /home/hadoop/liurui/log_stop_flag;sh /home/hadoop/liurui/github/iotdb-benchmark/ser-benchmark.sh > /dev/null 2>&1 &"
echo '------Client Test Begin Time------'
date
cd bin
sh startup.sh -cf ../conf/config.properties
wait
ssh hadoop@192.168.130.16 "touch /home/hadoop/liurui/log_stop_flag"
echo '------Client Test Complete Time------'
date

#initial database in server
ssh hadoop@192.168.130.16 "sh /home/hadoop/xuyi/iotdb/iotdb/bin/stop-server.sh;rm -rf data;sh /home/hadoop/xuyi/iotdb/iotdb/bin/start-server.sh"

