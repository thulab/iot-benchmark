#!/bin/bash

if [ -z "${BENCHMARK_HOME}" ]; then
  export BENCHMARK_HOME="$(cd "`dirname "$0"`"; pwd)"
fi

if [ $# -gt 0 ]; then
  exec $BENCHMARK_HOME/bin/startup.sh "$@"
else
  exec $BENCHMARK_HOME/bin/startup.sh
fi
exit $?