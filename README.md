# Usage

## Dependencies

* JDK >= 1.8
* Maven >= 3.0

## How to install dependencies

### install tsfile

```sh
git clone https://github.com/thulab/tsfile.git
cd tsfile/
mvn clean install -Dmaven.test.skip=true
```

### install iotdb-jdbc

```sh
git clone https://github.com/thulab/iotdb-jdbc.git
cd iotdb-jdbc/
mvn clean install -Dmaven.test.skip=true
```

## How to run

```sh
cd iotdb-benchmark
./benchmark.sh
```