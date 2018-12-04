#!/bin/sh

if [ -z "${BENCHMARK_HOME}" ]; then
  export BENCHMARK_HOME="$(cd "`dirname "$0"`"/.; pwd)"
fi

HOST_NAME=liurui
IP=$(grep "HOST" $BENCHMARK_HOME/conf/config.properties)
FLAG_AND_DATA_PATH=$(grep "LOG_STOP_FLAG_PATH" $BENCHMARK_HOME/conf/config.properties)
SERVER_HOST=$HOST_NAME@${IP#*=}
LOG_STOP_FLAG_PATH=${FLAG_AND_DATA_PATH#*=}

git pull
COMMIT_ID=$(ssh $SERVER_HOST "cd $LOG_STOP_FLAG_PATH/iotdb;git tag -l | tail -n 1")" commit_id:"$(ssh $SERVER_HOST "cd $LOG_STOP_FLAG_PATH/iotdb;git rev-parse HEAD")
sed -i "s/^VERSION.*$/VERSION=${COMMIT_ID}/g" $BENCHMARK_HOME/conf/config.properties
rm -rf ./lib
mvn clean package -Dmaven.test.skip=true
cd bin
sh startup.sh -cf ../conf/config.properties
