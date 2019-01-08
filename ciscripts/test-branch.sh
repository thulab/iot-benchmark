#!/bin/sh

if [ -z "${BENCHMARK_HOME}" ]; then
  BENCHMARK_HOME="$(cd "`dirname "$0"`"/.; pwd)"
  export BENCHMARK_HOME="$(cd $BENCHMARK_HOME/..; pwd)"
fi


sh $BENCHMARK_HOME/ciscripts/rep-compare-branch.sh fit/ingestion-overflow50-auto-test iotdbconf/fit false master ~/.m2/repository/cn