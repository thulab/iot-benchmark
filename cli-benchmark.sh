#!/bin/sh

mvn clean package -Dmaven.test.skip=true

ssh hadoop@192.168.130.16 'sh /home/hadoop/liurui/github/iotdb-benchmark/ser-benchmark.s > /dev/null 2>&1 &'
echo '------Client Test Begin Time------'
date
cd bin
sh startup.sh -cf ../conf/config.properties
wait
ssh hadoop@192.168.130.16 'touch /home/hadoop/liurui/log_stop_flag'
echo '------Client Test Complete Time------'
date
