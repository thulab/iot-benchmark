#!/bin/sh

if [ -z "${BENCHMARK_HOME}" ]; then
  export BENCHMARK_HOME="$(cd "`dirname "$0"`"/.; pwd)"
fi

HOST_NAME=liurui
IP=$(grep "HOST" $BENCHMARK_HOME/conf/config.properties)
FLAG_AND_DATA_PATH=$(grep "LOG_STOP_FLAG_PATH" $BENCHMARK_HOME/conf/config.properties)
SERVER_HOST=$HOST_NAME@${IP#*=}
LOG_STOP_FLAG_PATH=${FLAG_AND_DATA_PATH#*=}
IS_TEST_BASELINE=$1



if [ $IS_TEST_BASELINE = "true" ]; then
    cp ./archive/pom/baseline_pom.xml  ./pom.xml
    COMMIT_ID="BASELINE_commit_id:"$(ssh $SERVER_HOST "cd $LOG_STOP_FLAG_PATH/baseline_iotdb/iotdb;git rev-parse HEAD")
    sed -i "s/^VERSION.*$/VERSION=${COMMIT_ID}/g" $BENCHMARK_HOME/conf/config.properties
else
    cp ./archive/pom/pom.xml  ./pom.xml
    COMMIT_ID="commit_id:"$(ssh $SERVER_HOST "cd $LOG_STOP_FLAG_PATH/incubator-iotdb;git rev-parse HEAD")
    sed -i "s/^VERSION.*$/VERSION=${COMMIT_ID}/g" $BENCHMARK_HOME/conf/config.properties
fi
rm -rf ./lib
mvn clean package -Dmaven.test.skip=true
cd bin
sh startup.sh -cf ../conf/config.properties
