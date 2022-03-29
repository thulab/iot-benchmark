#!/bin/sh

if [ -z "${BENCHMARK_HOME}" ]; then
  export BENCHMARK_HOME="$(cd "`dirname "$0"`"/.; pwd)"
fi

# IMPORTANT: to use this script, make sure you have password-free ssh access to 127.0.0.1 and the server of database service
# if not you need to use the following command:
# ssh-keygen -t rsa
# ssh-copy-id [hostname]@[IP]

# configure the related path and host name
IOTDB_HOME=/home/liurui/github/iotdb/iotdb/bin
REMOTE_BENCHMARK_HOME=/home/admin/git/iotdb-benchmark/influxdb/target/influxdb-0.0.1
USER_NAME=admin
SSH_PORT=22

#extract parameters from config.properties
IP=$(grep "^HOST" $BENCHMARK_HOME/conf/config.properties)
BENCHMARK_WORK_MODE=$(grep "BENCHMARK_WORK_MODE" $BENCHMARK_HOME/conf/config.properties)
DB=$(grep "^DB_SWITCH" $BENCHMARK_HOME/conf/config.properties)
SERVER_HOST=$USER_NAME@${IP#*=}

echo Testing ${DB#*=} ...

# If IP is localhost not trigger off server mode
if [ "${IP#*=}" != "127.0.0.1" ]; then
    ssh -p $SSH_PORT $SERVER_HOST "rm $REMOTE_BENCHMARK_HOME/conf/config.properties"
    scp -P $SSH_PORT $BENCHMARK_HOME/conf/config.properties $SERVER_HOST:$REMOTE_BENCHMARK_HOME/conf
    echo Start server system information recording ...
    SERVER_PID=$(ssh -p $SSH_PORT $SERVER_HOST "sh $REMOTE_BENCHMARK_HOME/ser-benchmark.sh > /dev/null 2>&1 & echo \$!")
    echo ServerMode started on $IP with PID: $SERVER_PID
fi

# Init IoTDB
if [ "${DB#*=}" = "IoTDB" -a "${BENCHMARK_WORK_MODE#*=}" = "testWithDefaultPath" ]; then
   echo "initial database in server..."
   ssh -p $SSH_PORT $SERVER_HOST "cd $DB_DATA_PATH;rm -rf data;sh $IOTDB_HOME/stop-server.sh;sleep 2"
   echo 'wait a few seconds for launching IoTDB...'
   IOTDB_PID=$(ssh -p $SSH_PORT $SERVER_HOST "cd $DB_DATA_PATH;sh $IOTDB_HOME/start-server.sh > /dev/null 2>&1 & echo \$!")
   sleep 20
   echo IOTDB started on $IP with PID: $IOTDB_PID
fi

echo '------Client Test Begin Time------'
date
cd bin
sh startup.sh -cf ../conf/config.properties
wait
if [ "${IP#*=}" != "127.0.0.1" ]; then
    echo Stop server system information recording ...
    ssh -p $SSH_PORT $SERVER_HOST "kill $SERVER_PID"
fi

echo '------Client Test Complete Time------'
date