#!/bin/sh

git pull

mvn clean package -Dmaven.test.skip=true

cd bin

./startup.sh -cf ../conf/config.properties