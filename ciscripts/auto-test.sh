#!/bin/sh

ROUTINE=$1
IOTDB_CONF=$2
IS_BASELINE=$3
DATABASE=$4
MYSQL_HOST=$5

git pull
echo "begin testing the performance..."
cp ./archive/$ROUTINE  ./routine
./rep-benchmark.sh $IOTDB_CONF $IS_BASELINE
mysql -A $DATABASE -h $MYSQL_HOST -uroot -pIse_Nel_2017  -e "SELECT queryResult.projectID,queryResult.avg,queryResult.midAvg,queryResult.min,queryResult.max,queryResult.p1,queryResult.p5,queryResult.p50,queryResult.p90,queryResult.p95,queryResult.totalTimes,queryResult.totalPoints,queryResult.queryNumber from queryResult;" | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' > queryResult.csv
#tail -n 3 ./logs/log_info.log | cut -d '-' -f 4 > result.txt