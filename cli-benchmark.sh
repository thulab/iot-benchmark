#!/bin/sh

mvn clean package -Dmaven.test.skip=true

ssh hadoop@192.168.130.16 'cd /home/hadoop/liurui/github/iotdb-benchmark;sh ser-benchmark.sh'
echo '------Client Test Begin Time:------'
date
cd bin
sh startup.sh -cf ../conf/config.properties
wait
ssh hadoop@192.168.130.16 'cd /home/hadoop/liurui;touch log_stop_flag'
echo '------Client Test Complete Time------'
date
