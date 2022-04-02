#!/bin/sh

if [ -z "${BENCHMARK_HOME}" ]; then
  export BENCHMARK_HOME="$(cd "`dirname "$0"`"; pwd)"
fi

exec $BENCHMARK_HOME/bin/startup.sh -cf $BENCHMARK_HOME/conf/config.properties

exit $?