#!/bin/sh

if [ -z "${BENCHMARK_HOME}" ]; then
  export BENCHMARK_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

echo $BENCHMARK_HOME

MAIN_CLASS=cn.edu.tsinghua.iotdb.benchmark.App

CLASSPATH=""
for f in ${BENCHMARK_HOME}/lib/*.jar; do
  CLASSPATH=${CLASSPATH}":"$f
done


if [ -n "$JAVA_HOME" ]; then
    for java in "$JAVA_HOME"/bin/amd64/java "$JAVA_HOME"/bin/java; do
        if [ -x "$java" ]; then
            JAVA="$java"
            break
        fi
    done
else
    JAVA=java
fi

pid=$$


echo "================="
echo "the job pid is ${pid}"
echo "================="

mkdir ${pid}

cd ${pid}

pidstat -p ${pid} -u  5 > cpu.log &
pidstat -p ${pid} -r  5 > mem.log &
pidstat -p ${pid} -w 5 > context.log &
iostat -x 5 > iotstat.log &

cd ..


exec "$JAVA" -Duser.timezone=GMT+8 -Dlogback.configurationFile=${BENCHMARK_HOME}/conf/logback.xml  -cp "$CLASSPATH" "$MAIN_CLASS" "$@"

#====== monitor kill code

killpid=$(ps -ux | grep "liurui" | grep "iostat" -m 1 | awk '{print $2}')

echo "========iostat pid======="
echo ${killpid}

kill -9 ${killpid}

killpid=$(ps -ux | grep "liurui" | grep "pidstat" -m 1 | awk '{print $2}')

kill -9 ${killpid}

killpid=$(ps -ux | grep "liurui" | grep "pidstat" -m 1 | awk '{print $2}')

kill -9 ${killpid}

killpid=$(ps -ux | grep "liurui" | grep "pidstat" -m 1 | awk '{print $2}')

kill -9 ${killpid}

echo "================="
echo "the job pid is ${pid}"
echo "================="

#===== monitor kill code

exit $?