#!/bin/sh

if [ -z "${BENCHMARK_HOME}" ]; then
  BENCHMARK_HOME="$(cd "`dirname "$0"`"/.; pwd)"
  export BENCHMARK_HOME="$(cd $BENCHMARK_HOME/..; pwd)"
fi
#IMPORTANT: to use this script, make sure you have password-free ssh access to 127.0.0.1 and the server of database service
# if not you need to use the following command:
# ssh-keygen -t rsa
# ssh-copy-id [hostname]@[IP]

#configure the related path and host name

HOST_NAME=liurui
IS_SSH_CHANGE_PORT=false
SSH_PORT=2222
IOTDB_CONF=$1
IS_TEST_BASELINE=$2
BRANCH=$3
MAVEN_CLEAN_REPO_PATH=$4

#extract parameters from config.properties
IP=$(grep "HOST" $BENCHMARK_HOME/conf/config.properties)
FLAG_AND_DATA_PATH=$(grep "LOG_STOP_FLAG_PATH" $BENCHMARK_HOME/conf/config.properties)
SERVER_HOST=$HOST_NAME@${IP#*=}
LOG_STOP_FLAG_PATH=${FLAG_AND_DATA_PATH#*=}
CLIENT_LOG_STOP_FLAG_PATH_LINE=$(grep "LOG_STOP_FLAG_PATH" $BENCHMARK_HOME/conf/clientSystemInfo.properties)
CLIENT_LOG_STOP_FLAG_PATH=${CLIENT_LOG_STOP_FLAG_PATH_LINE#*=}



#get mode parameter
#sed -i 's/SERVER_MODE *= *true/SERVER_MODE=false/g' $BENCHMARK_HOME/conf/config.properties
BENCHMARK_WORK_MODE=$(grep "BENCHMARK_WORK_MODE" $BENCHMARK_HOME/conf/config.properties)
DB=$(grep "DB_SWITCH" $BENCHMARK_HOME/conf/config.properties)

echo Testing ${DB#*=} ...

rm -rf ./lib
if [ $IS_TEST_BASELINE = "true" ]; then
    cp $BENCHMARK_HOME/archive/pom/baseline_pom.xml  $BENCHMARK_HOME/pom.xml
else
    cp $BENCHMARK_HOME/archive/pom/pom.xml  $BENCHMARK_HOME/pom.xml
    rm -rf $MAVEN_CLEAN_REPO_PATH
    rm -rf $BENCHMARK_HOME/iotdb-$BRANCH
    mkdir $BENCHMARK_HOME/iotdb-$BRANCH
    cd $BENCHMARK_HOME/iotdb-$BRANCH
    git clone https://github.com/apache/incubator-iotdb.git
    cd $BENCHMARK_HOME/iotdb-$BRANCH/incubator-iotdb
    git checkout $BRANCH
    mvn clean install -Dmaven.test.skip=true
    echo "install incubator-iotdb locally finished"
fi
cd $BENCHMARK_HOME
mvn clean package -Dmaven.test.skip=true

#prepare for client system info recording benchmark
#if [ -d $CLIENT_LOG_STOP_FLAG_PATH ]; then
##    MYSQL_URL_LINE=$(grep "MYSQL_URL" $BENCHMARK_HOME/conf/config.properties)
##    MYSQL_URL_VALUE=${MYSQL_URL_LINE#*=}
##    IS_USE_MYSQL_LINE=$(grep "IS_USE_MYSQL" $BENCHMARK_HOME/conf/config.properties)
##    IS_USE_MYSQL_VALUE=${IS_USE_MYSQL_LINE#*=}
##    sed -i "s/^MYSQL_URL.*$/MYSQL_URL=${MYSQL_URL_VALUE}/g" $CLIENT_LOG_STOP_FLAG_PATH/iotdb-benchmark/conf/clientSystemInfo.properties
##    sed -i "s/^IS_USE_MYSQL.*$/MYSQL_URL=${IS_USE_MYSQL_VALUE}/g" $CLIENT_LOG_STOP_FLAG_PATH/iotdb-benchmark/conf/clientSystemInfo.properties
##
#    ssh $HOST_NAME@127.0.0.1 "sh $CLIENT_LOG_STOP_FLAG_PATH/iotdb-benchmark/ser_cli-benchmark.sh > /dev/null 2>&1 &"
#    else
#    ssh $HOST_NAME@127.0.0.1 "mkdir $CLIENT_LOG_STOP_FLAG_PATH;cp -r ${BENCHMARK_HOME} $CLIENT_LOG_STOP_FLAG_PATH"
#    ssh $HOST_NAME@127.0.0.1 "sh $CLIENT_LOG_STOP_FLAG_PATH/iotdb-benchmark/ser_cli-benchmark.sh > /dev/null 2>&1 &"
#fi

#synchronize config server benchmark
if [ "${IS_SSH_CHANGE_PORT#*=}" = "true" ]; then
    ssh -p $SSH_PORT $SERVER_HOST "rm $LOG_STOP_FLAG_PATH/conf/config.properties"
    scp -P $SSH_PORT $BENCHMARK_HOME/conf/config.properties $SERVER_HOST:$LOG_STOP_FLAG_PATH/conf
    ssh -p $SSH_PORT $SERVER_HOST "sh $LOG_STOP_FLAG_PATH/ser-benchmark.sh > /dev/null 2>&1 &"

    if [ "${DB#*=}" = "IoTDB" -a "${BENCHMARK_WORK_MODE#*=}" = "insertTestWithDefaultPath" ]; then
        COMMIT_ID=$(ssh -p $SSH_PORT $SERVER_HOST "cd $LOG_STOP_FLAG_PATH;git rev-parse HEAD")
        sed -i "s/^VERSION.*$/VERSION=${COMMIT_ID}/g" $BENCHMARK_HOME/conf/config.properties
        echo "initial database in server..."
        ssh -p $SSH_PORT $SERVER_HOST "sh $LOG_STOP_FLAG_PATH/incubator-iotdb/iotdb/iotdb/sbin/stop-server.sh;sleep 5"
        ssh -p $SSH_PORT $SERVER_HOST "cd $LOG_STOP_FLAG_PATH;rm -rf ./*;git clone https://github.com/apache/incubator-iotdb.git;cd ./incubator-iotdb;mvn clean package -Dmaven.test.skip=true"
        ssh -p $SSH_PORT $SERVER_HOST "sh $LOG_STOP_FLAG_PATH/incubator-iotdb/iotdb/iotdb/sbin/start-server.sh > /dev/null 2>&1 &"
        echo 'wait a few seconds for launching IoTDB...'
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
        echo "Start to copy iotdb-benchmark to remote server for monitoring."
        ssh $SERVER_HOST "rm -rf $LOG_STOP_FLAG_PATH/iotdb-benchmark"
        scp -r $BENCHMARK_HOME $SERVER_HOST:$LOG_STOP_FLAG_PATH
        echo "Remote server iotdb-benchmark transferring complete."
#        ssh $SERVER_HOST "rm $LOG_STOP_FLAG_PATH/iotdb-benchmark/conf/config.properties"
#        scp $BENCHMARK_HOME/conf/config.properties $SERVER_HOST:$LOG_STOP_FLAG_PATH/iotdb-benchmark/conf
#        echo "Replace config.properties for server monitoring complete."
    fi

    if [ "${DB#*=}" = "IoTDB" -a "${BENCHMARK_WORK_MODE#*=}" = "insertTestWithDefaultPath" ]; then
        echo "initial database in server..."
        if [ $IS_TEST_BASELINE = "true" ]; then
            echo "Testing baseline IoTDB version: v0.7.0"
            ssh $SERVER_HOST "cd $LOG_STOP_FLAG_PATH;rm -rf ./baseline_iotdb;mkdir ./baseline_iotdb;cd ./baseline_iotdb;git clone https://github.com/apache/incubator-iotdb.git;cd ./incubator-iotdb;git checkout v0.7.0;mvn clean package -Dmaven.test.skip=true"
            ssh $SERVER_HOST "sh $LOG_STOP_FLAG_PATH/baseline_iotdb/incubator-iotdb/iotdb/bin/stop-server.sh;sleep 5"
            #start server system information recording
            ssh $SERVER_HOST "sh $LOG_STOP_FLAG_PATH/iotdb-benchmark/ser-benchmark.sh > /dev/null 2>&1 &"
            COMMIT_ID=$(ssh $SERVER_HOST "cd $LOG_STOP_FLAG_PATH/baseline_iotdb/incubator-iotdb;git tag -l | tail -n 1")"BASELINE_commit_id:"$(ssh $SERVER_HOST "cd $LOG_STOP_FLAG_PATH/baseline_iotdb/incubator-iotdb;git rev-parse HEAD")
            sed -i "s/^VERSION.*$/VERSION=${COMMIT_ID}/g" $BENCHMARK_HOME/conf/config.properties
            scp $BENCHMARK_HOME/$IOTDB_CONF/iotdb-engine.properties $SERVER_HOST:$LOG_STOP_FLAG_PATH/baseline_iotdb/incubator-iotdb/iotdb/conf
            scp $BENCHMARK_HOME/$IOTDB_CONF/iotdb-env.sh $SERVER_HOST:$LOG_STOP_FLAG_PATH/baseline_iotdb/incubator-iotdb/iotdb/conf
            ssh $SERVER_HOST "sh $LOG_STOP_FLAG_PATH/baseline_iotdb/incubator-iotdb/iotdb/bin/start-server.sh > /dev/null 2>&1 &"
        else
            echo "Testing IoTDB branch: $BRANCH"
            echo "Start to install IoTDB branch $BRANCH on remote server"
            ssh $SERVER_HOST "rm -rf $LOG_STOP_FLAG_PATH/iotdb-$BRANCH;mkdir $LOG_STOP_FLAG_PATH/iotdb-$BRANCH;cd $LOG_STOP_FLAG_PATH/iotdb-$BRANCH;git clone https://github.com/apache/incubator-iotdb.git;cd ./incubator-iotdb;git checkout $BRANCH;mvn clean package -Dmaven.test.skip=true"
            echo "Install IoTDB branch $BRANCH complete"
            echo "Stop existing IoTDB service on remote server"
            ssh $SERVER_HOST "sh $LOG_STOP_FLAG_PATH/iotdb-$BRANCH/incubator-iotdb/iotdb/iotdb/sbin/stop-server.sh;sleep 5"
            echo "Existing IoTDB service on remote server stopped"
            echo "Start server system information recording"
            ssh $SERVER_HOST "sh $LOG_STOP_FLAG_PATH/iotdb-benchmark/ser-benchmark.sh > /dev/null 2>&1 &"
            COMMIT_ID=$(ssh $SERVER_HOST "cd $LOG_STOP_FLAG_PATH/iotdb-$BRANCH/incubator-iotdb;git tag -l | tail -n 1")"_commit_id:"$(ssh $SERVER_HOST "cd $LOG_STOP_FLAG_PATH/iotdb-$BRANCH/incubator-iotdb;git rev-parse HEAD")
            sed -i "s/^VERSION.*$/VERSION=${COMMIT_ID}/g" $BENCHMARK_HOME/conf/config.properties
            echo "Setting IoTDB configuration including iotdb-engine.properties and iotdb-env.sh"
            scp $BENCHMARK_HOME/$IOTDB_CONF/iotdb-engine.properties $SERVER_HOST:$LOG_STOP_FLAG_PATH/iotdb-$BRANCH/incubator-iotdb/iotdb/iotdb/conf
            scp $BENCHMARK_HOME/$IOTDB_CONF/iotdb-env.sh $SERVER_HOST:$LOG_STOP_FLAG_PATH/iotdb-$BRANCH/incubator-iotdb/iotdb/iotdb/conf
            echo "Start IoTDB service on remote server"
            ssh $SERVER_HOST "bash $LOG_STOP_FLAG_PATH/iotdb-$BRANCH/incubator-iotdb/iotdb/iotdb/sbin/start-server.sh > /dev/null 2>&1 &"
        fi
        echo 'wait a few seconds for launching IoTDB...'
        sleep 40
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


echo '------Client Test Complete Time------'
date
