#!/bin/sh

if [ -z "${BENCHMARK_HOME}" ]; then
  BENCHMARK_HOME="$(cd "`dirname "$0"`"/.; pwd)"
  export BENCHMARK_HOME="$(cd $BENCHMARK_HOME/..; pwd)"
fi

ROUTINE=$1
IOTDB_CONF=$2
IS_BASELINE=$3
BRANCH=$4
MAVEN_CLEAN_REPO_PATH=$5

git pull
echo "begin testing the performance..."
cp $BENCHMARK_HOME/archive/$ROUTINE  $BENCHMARK_HOME/routine
sh $BENCHMARK_HOME/ciscripts/branch-rep.sh $IOTDB_CONF $IS_BASELINE $BRANCH $MAVEN_CLEAN_REPO_PATH
tail -n 3 $BENCHMARK_HOME/logs/log_info.log | cut -d '-' -f 4 >> result.txt