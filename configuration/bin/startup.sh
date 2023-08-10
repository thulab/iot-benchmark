#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#


if [ -z "${BENCHMARK_HOME}" ]; then
  export BENCHMARK_HOME="$(cd "`dirname "$0"`"/.. && pwd)"
fi

echo Set BENCHMARK_HOME=$BENCHMARK_HOME

MAIN_CLASS=cn.edu.tsinghua.iot.benchmark.App

CLASSPATH=""
for f in ${BENCHMARK_HOME}/lib/*.jar; do
  CLASSPATH=${CLASSPATH}":"$f
done

if [ -n "$JAVA_HOME" ]; then
    for java in "$JAVA_HOME"/bin/java "$JAVA_HOME"/bin/amd64/java; do
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

# Maximum heap size
#MAX_HEAP_SIZE="2G"
# Minimum heap size
#HEAP_NEWSIZE="2G"

while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
    -cf)
      benchmark_conf="$2"
      ;;
  esac
  shift
done

if [ -z $benchmark_conf ] ; then
  benchmark_conf=${BENCHMARK_HOME}/conf/config.properties
else
  benchmark_conf="$(cd "$(dirname "$benchmark_conf")" && pwd)/$(basename "$benchmark_conf")"
fi
echo Using configuration file: $benchmark_conf

benchmark_parms="$benchmark_parms -Duser.timezone=GMT+8"
benchmark_parms="$benchmark_parms -Dlogback.configurationFile=${BENCHMARK_HOME}/conf/logback.xml"

if [ -n $MAX_HEAP_SIZE ] && [ -n "$HEAP_NEWSIZE" ];then
  echo Set MAX_HEAP_SIZE=$MAX_HEAP_SIZE, HEAP_NEWSIZE=$HEAP_NEWSIZE
  benchmark_parms="$benchmark_parms -Xms${HEAP_NEWSIZE} -Xmx${MAX_HEAP_SIZE}"
else
  echo Using default memory configuration to startup.

fi

exec "$JAVA" $benchmark_parms -cp "$CLASSPATH" "$MAIN_CLASS" -cf "$benchmark_conf"

exit $?