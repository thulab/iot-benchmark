#!/bin/sh

if [ -z "${BENCHMARK_HOME}" ]; then
  export BENCHMARK_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

echo $BENCHMARK_HOME

MAIN_CLASS=cn.edu.tsinghua.iotdb.benchmark.IoTDBWriteBatchMain

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

SESSION_HOST=127.0.0.1
SESSION_PORT=6667
SESSION_USER=root
SESSION_PASSWORD=root
KAFKA_GROUP=g1

exec "$JAVA" -Duser.timezone=GMT+8 -Dlogback.configurationFile=${BENCHMARK_HOME}/conf/logback.xml  -cp "$CLASSPATH" "$MAIN_CLASS" "$SESSION_HOST" "$SESSION_PORT" "$SESSION_USER" "$SESSION_PASSWORD" "$KAFKA_GROUP"

exit $?