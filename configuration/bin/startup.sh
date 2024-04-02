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

# Maximum heap size
#MAX_HEAP_SIZE="2G"
# Minimum heap size
#HEAP_NEWSIZE="2G"

show_help() {
  echo "usage: benchmark.sh [-cf configuration_file] [-heapsize HEAP_SIZE] [-maxheapsize MAX_HEAP_SIZE]"
  echo " -h           Show help."
  echo " -cf          Specify configuration file."
  echo " -heapsize    Specify HEAP_SIZE."
  echo " -maxheapsize Specify MAX_HEAP_SIZE."
  echo "example: ./benchmark.sh -cf conf -heapsize 1G -maxheapsize 2G"
}

while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
    -h|--help)
      show_help
      exit 0
      ;;
    -cf)
      benchmark_conf="$2"
      shift
      shift
      ;;
    -maxheapsize)
      MAX_HEAP_SIZE="$2"
      shift
      shift
      ;;
    -heapsize)
      HEAP_NEWSIZE="$2"
      shift
      shift
      ;;
    *)
      echo "unknown: $key"
      exit 1
      ;;
  esac
done

# check java
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

# check BENCHMARK_HOME
if [ -z "${BENCHMARK_HOME}" ]; then
  export BENCHMARK_HOME="$(cd "$(dirname "$0")/.." && pwd)"
fi

# check $benchmark_conf
if [ -z "${benchmark_conf}" ] ; then
  benchmark_conf=${BENCHMARK_HOME}/conf
else
  benchmark_conf="$(cd "$(dirname "$benchmark_conf")" && pwd)/$(basename "$benchmark_conf")"
  if [ ! -e "$benchmark_conf" ]; then
    echo "The file $benchmark_conf does not exist."
    exit 1
  fi
fi
echo Using configuration file: "${benchmark_conf}"

# set MAIN_CLASS
MAIN_CLASS=cn.edu.tsinghua.iot.benchmark.App
# set CLASSPATH
CLASSPATH=""
for f in ${BENCHMARK_HOME}/lib/*.jar; do
  CLASSPATH=${CLASSPATH}":"$f
done

# set benchmark_parms
benchmark_parms="$benchmark_parms -Duser.timezone=GMT+8"
benchmark_parms="$benchmark_parms -Dlogback.configurationFile=${benchmark_conf}/logback.xml"
if [ -n "$MAX_HEAP_SIZE" ]; then
  echo Set MAX_HEAP_SIZE=$MAX_HEAP_SIZE
  benchmark_parms="$benchmark_parms -Xmx${MAX_HEAP_SIZE}"
fi
if [ -n "$HEAP_NEWSIZE" ]; then
  echo Set HEAP_NEWSIZE=$HEAP_NEWSIZE
  benchmark_parms="$benchmark_parms -Xms${HEAP_NEWSIZE}"
fi
if [ -z $MAX_HEAP_SIZE ] && [ -z "$HEAP_NEWSIZE" ]; then
  echo Using default memory configuration to startup.
fi

# startup
exec "$JAVA" $benchmark_parms -cp "$CLASSPATH" "$MAIN_CLASS" -cf "$benchmark_conf"

exit $?