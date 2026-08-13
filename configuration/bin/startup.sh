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
# all-in-one 布局（存在 lib/core 目录）时：
#   先加载 DB_SWITCH 对应的专属目录 lib/<db>，再加载 lib/core，
#   保证各模块优先使用自己版本的同名依赖；FakeDB/SelfCheck 仅用 lib/core。
#   用 classpath 通配符（lib/<dir>/*）由 java 自行展开 jar；
#   Windows（Git Bash/MSYS）下路径转 C:/ 形式、分隔符用分号，Linux/macOS 用冒号。
# 旧布局（lib 直接放 jar）保持原行为。
CLASSPATH=""
case "$(uname -s 2>/dev/null)" in
  MINGW*|MSYS*)
    CP_SEP=";"
    ;;
  *)
    CP_SEP=":"
    ;;
esac
if [ -d "${BENCHMARK_HOME}/lib/core" ]; then
  CONF_FILE="${benchmark_conf}"
  if [ -d "${benchmark_conf}" ]; then
    CONF_FILE="${benchmark_conf}/config.properties"
  fi
  DB_SWITCH=$(grep -E '^DB_SWITCH=' "${CONF_FILE}" 2>/dev/null | head -n1 | cut -d'=' -f2- | tr -d ' ')
  if [ -z "${DB_SWITCH}" ]; then
    DB_SWITCH=$(grep -E '^#[[:space:]]*DB_SWITCH=' "${CONF_FILE}" 2>/dev/null | head -n1 | sed 's/^#[[:space:]]*//' | cut -d'=' -f2- | tr -d ' ')
  fi
  DB_LIB_DIR=""
  case "${DB_SWITCH}" in
    IoTDB-200-*) DB_LIB_DIR="iotdb-2.0" ;;
    IoTDB-130-*) DB_LIB_DIR="iotdb-1.3" ;;
    InfluxDB-2*) DB_LIB_DIR="influxdb-2.0" ;;
    InfluxDB*)   DB_LIB_DIR="influxdb" ;;
    OpenTSDB*)   DB_LIB_DIR="opentsdb" ;;
    CnosDB*)     DB_LIB_DIR="cnosdb" ;;
    KairosDB*)   DB_LIB_DIR="kairosdb" ;;
    TimescaleDB-cluster*) DB_LIB_DIR="timescaledb-cluster" ;;
    TimescaleDB*) DB_LIB_DIR="timescaledb" ;;
    TDengine-3*) DB_LIB_DIR="tdengine-3.0" ;;
    TDengine*)   DB_LIB_DIR="tdengine" ;;
    QuestDB*)    DB_LIB_DIR="questdb" ;;
    MsSqlServer*) DB_LIB_DIR="mssqlserver" ;;
    VictoriaMetrics*) DB_LIB_DIR="victoriametrics" ;;
    DolphinDB-3*) DB_LIB_DIR="dolphindb-3.0" ;;
    DolphinDB-2*) DB_LIB_DIR="dolphindb-2.0" ;;
    SQLite*)     DB_LIB_DIR="sqlite" ;;
    *)           DB_LIB_DIR="" ;;
  esac
  CP_HOME="${BENCHMARK_HOME}"
  if [ "${CP_SEP}" = ";" ]; then
    CP_HOME="$(cd "${BENCHMARK_HOME}" && pwd -W)"
  fi
  CP_DB_LIBS=""
  if [ -n "${DB_LIB_DIR}" ] && [ -d "${BENCHMARK_HOME}/lib/${DB_LIB_DIR}" ]; then
    CP_DB_LIBS="${CP_HOME}/lib/${DB_LIB_DIR}/*"
  fi
  # conf 目录也放入 classpath：logback.xml 有 -Dlogback.configurationFile 显式指定，
  # 但 SLF4J 选中 reload4j（log4j 1.2）binding 的模块（如 iotdb-2.0/1.3，其 lib 里的
  # logback-classic 1.3.x 对 slf4j-api 1.7 不可见）需要 classpath 上的 log4j.properties
  # 才有 appender，否则 LOGGER 输出（含 Latency 矩阵）被吞。
  if [ -n "${CP_DB_LIBS}" ]; then
    CLASSPATH="${CP_DB_LIBS}${CP_SEP}${CP_HOME}/lib/core/*${CP_SEP}${CP_HOME}/conf"
  else
    CLASSPATH="${CP_HOME}/lib/core/*${CP_SEP}${CP_HOME}/conf"
  fi
else
  CP_HOME="${BENCHMARK_HOME}"
  if [ "${CP_SEP}" = ";" ]; then
    CP_HOME="$(cd "${BENCHMARK_HOME}" && pwd -W)"
  fi
  for f in ${CP_HOME}/lib/*.jar; do
    CLASSPATH=${CLASSPATH}${CP_SEP}$f
  done
fi

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