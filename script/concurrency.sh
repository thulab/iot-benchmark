#!/bin/sh

if [ -z "${BENCHMARK_HOME}" ]; then
    export BENCHMARK_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

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

if [ -z $JAVA ] ; then
    echo Unable to find java executable. Check JAVA_HOME and PATH environment variables.  > /dev/stderr
    exit 1;
fi

CLASSPATH=""
for f in ${BENCHMARK_HOME}/lib/*.jar; do
    CLASSPATH=${CLASSPATH}":"$f
done

MAIN_CLASS=cn.edu.tsinghua.iotdb.benchmark.concurrency.App

"$JAVA" -cp "$CLASSPATH" "$MAIN_CLASS" "$@"
exit $?