#!/bin/bash

if [ -z "${BENCHMARK_HOME}" ]; then
  export BENCHMARK_HOME="$(cd "`dirname "$0"`"; pwd)"
fi

if [ -n "$1" ]; then
  exec $BENCHMARK_HOME/bin/startup.sh -cf "$1"
else
  exec $BENCHMARK_HOME/bin/startup.sh -cf $BENCHMARK_HOME/conf/config.properties
fi
exit $?