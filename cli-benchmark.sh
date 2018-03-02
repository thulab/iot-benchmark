#!/bin/sh

if [ -z "${BENCHMARK_HOME}" ]; then
  export BENCHMARK_HOME="$(cd "`dirname "$0"`"/.; pwd)"
fi

#configure the related path and host name
IOTDB_HOME=/home/liurui/github/iotdb/iotdb/bin
REMOTE_BENCHMARK_HOME=/home/liurui/github/iotdb-benchmark
HOST_NAME=liurui
IS_SSH_CHANGE_PORT=false
SSH_PORT=2222


#extract parameters from config.properties
IP=$(grep "HOST" $BENCHMARK_HOME/conf/config.properties)
FLAG_AND_DATA_PATH=$(grep "LOG_STOP_FLAG_PATH" $BENCHMARK_HOME/conf/config.properties)
SERVER_HOST=$HOST_NAME@${IP#*=}
LOG_STOP_FLAG_PATH=${FLAG_AND_DATA_PATH#*=}

#get mode parameter
#sed -i 's/SERVER_MODE *= *true/SERVER_MODE=false/g' $BENCHMARK_HOME/conf/config.properties
BENCHMARK_WORK_MODE=$(grep "BENCHMARK_WORK_MODE" $BENCHMARK_HOME/conf/config.properties)
DB=$(grep "DB_SWITCH" $BENCHMARK_HOME/conf/config.properties)

echo Testing ${DB#*=} ...

mvn clean package -Dmaven.test.skip=true

#synchronize config server benchmark
if [ "${IS_SSH_CHANGE_PORT#*=}" = "true" ]; then
    ssh -p $SSH_PORT $SERVER_HOST "rm $REMOTE_BENCHMARK_HOME/conf/config.properties"
    scp -P $SSH_PORT $BENCHMARK_HOME/conf/config.properties $SERVER_HOST:$REMOTE_BENCHMARK_HOME/conf
    ssh -p $SSH_PORT $SERVER_HOST "sh $REMOTE_BENCHMARK_HOME/ser-benchmark.sh > /dev/null 2>&1 &"

    if [ "${DB#*=}" = "IoTDB" -a "${BENCHMARK_WORK_MODE#*=}" = "insertTestWithDefaultPath" ]; then
        COMMIT_ID=$(ssh -p $SSH_PORT $SERVER_HOST "cd $LOG_STOP_FLAG_PATH;git rev-parse HEAD")
        sed -i "s/^VERSION.*$/VERSION=${COMMIT_ID}/g" $BENCHMARK_HOME/conf/config.properties
        echo "initial database in server..."
        ssh -p $SSH_PORT $SERVER_HOST "cd $LOG_STOP_FLAG_PATH;rm -rf data;sh $IOTDB_HOME/stop-server.sh;sleep 2"
        ssh -p $SSH_PORT $SERVER_HOST "cd $LOG_STOP_FLAG_PATH;sh $IOTDB_HOME/start-server.sh > /dev/null 2>&1 &"
        echo 'wait a few seconds for lauching IoTDB...'
        sleep 20
    fi
    echo '------Client Test Begin Time------'
    date
    cd bin
    sh startup.sh -cf ../conf/config.properties
    wait
    #stop server system information recording
    ssh -p $SSH_PORT $SERVER_HOST "touch $LOG_STOP_FLAG_PATH/log_stop_flag"
else
    #If IP is localhost not trigger off server mode
    if [ "${IP#*=}" != "127.0.0.1" ]; then
        ssh $SERVER_HOST "rm $REMOTE_BENCHMARK_HOME/conf/config.properties"
        scp $BENCHMARK_HOME/conf/config.properties $SERVER_HOST:$REMOTE_BENCHMARK_HOME/conf
        #start server system information recording
        ssh $SERVER_HOST "sh $REMOTE_BENCHMARK_HOME/ser-benchmark.sh > /dev/null 2>&1 &"
    fi

    if [ "${DB#*=}" = "IoTDB" -a "${BENCHMARK_WORK_MODE#*=}" = "insertTestWithDefaultPath" ]; then
      COMMIT_ID=$(ssh $SERVER_HOST "cd $LOG_STOP_FLAG_PATH;git rev-parse HEAD")
      sed -i "s/^VERSION.*$/VERSION=${COMMIT_ID}/g" $BENCHMARK_HOME/conf/config.properties
      echo "initial database in server..."
      ssh $SERVER_HOST "cd $LOG_STOP_FLAG_PATH;rm -rf data;sh $IOTDB_HOME/stop-server.sh;sleep 2"
      ssh $SERVER_HOST "cd $LOG_STOP_FLAG_PATH;sh $IOTDB_HOME/start-server.sh > /dev/null 2>&1 &"
      echo 'wait a few seconds for lauching IoTDB...'
      sleep 20
    fi
    echo '------Client Test Begin Time------'
    date
    cd bin
    sh startup.sh -cf ../conf/config.properties
    wait
    if [ "${IP#*=}" != "127.0.0.1" ]; then
        #stop server system information recording
        ssh $SERVER_HOST "touch $LOG_STOP_FLAG_PATH/log_stop_flag"
    fi
fi

#ssh $SERVER_HOST "grep Statistic $LOG_STOP_FLAG_PATH/logs/log_info.log | tail -n 1 " >> $BENCHMARK_HOME/logs/MemoryMonitor.log

#if [ "${DB#*=}" = "IoTDB" -a "${BENCHMARK_WORK_MODE#*=}" = "insertTestWithDefaultPath" ]; then
    #ssh $SERVER_HOST "tail -n 2 $REMOTE_BENCHMARK_HOME/logs/log_info.log" >> $BENCHMARK_HOME/logs/log_info.log
    #ssh $SERVER_HOST "tail -n 1 $REMOTE_BENCHMARK_HOME/logs/log_info.log" >> $BENCHMARK_HOME/logs/log_result_info.txt
#fi


echo '------Client Test Complete Time------'
date

