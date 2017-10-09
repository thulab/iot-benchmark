#!/bin/sh

git pull

mvn clean package -Dmaven.test.skip=true

cd conf

./startup.sh -cf ../conf/config.properties