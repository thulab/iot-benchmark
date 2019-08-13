#!/bin/sh

git pull
echo "begin testing the performance..."
cp ./archive/fit/exact-query-auto-test  ./routine
./rep-benchmark.sh iotdbconf/fit false
mysql -A weekly_test -h 192.168.130.19 -P 3306 -uroot -pIse_Nel_2017  -e "SELECT queryResult.projectID,queryResult.avg,queryResult.midAvg,queryResult.min,queryResult.max,queryResult.p1,queryResult.p5,queryResult.p50,queryResult.p90,queryResult.p95,queryResult.totalTimes,queryResult.totalPoints,queryResult.queryNumber from queryResult;" | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' > queryResult.csv
#tail -n 3 ./logs/log_info.log | cut -d '-' -f 4 > result.txt