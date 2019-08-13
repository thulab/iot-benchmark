#!/bin/sh

git pull
echo "begin testing the performance..."
cp ./archive/fit/ingestion-overflow50-auto-test  ./routine
./rep-benchmark.sh iotdbconf/fit false
mysql -A auto_test -h 192.168.130.19 -P 3306 -uroot -pIse_Nel_2017  -e "SELECT insertResult.*, configInsertInfo.version from insertResult  JOIN configInsertInfo USING(projectID)" | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' > insertResult.csv
#tail -n 3 ./logs/log_info.log | cut -d '-' -f 4 > result.txt