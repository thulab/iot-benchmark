#!/bin/sh

cd /home/hadoop/liurui/github/iotdb-benchmark

mvn clean package -Dmaven.test.skip=true

echo '------Server Test Begin Time------'

date

cd bin

sh startup.sh -cf ../conf/config.properties

echo '------Server Test Complete Time------'

date
