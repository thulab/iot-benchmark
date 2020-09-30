#!/bin/sh

if [ -z "${BENCHMARK_HOME}" ]; then

  export BENCHMARK_HOME="$(cd "`dirname "$0"`"/.; pwd)"

fi

echo $BENCHMARK_HOME

#sed -i 's/SERVER_MODE *= *false/SERVER_MODE=true/g' $BENCHMARK_HOME/conf/config.properties
sed -i "s/^BENCHMARK_WORK_MODE.*$/BENCHMARK_WORK_MODE=serverMODE/g" $BENCHMARK_HOME/conf/config.properties

cd $BENCHMARK_HOME

echo '------Server Test Begin Time------'

date

cd bin

sh startup.sh -cf ../conf/config.properties

echo '------Server Test Complete Time------'

date


