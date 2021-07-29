Benchmark influxdb
---
This project is using iotdb-benchmark to test influxdb 2

# environment
1. influxdb: 2.0.7, using docker

# database setup
1. `docker pull influxdb:2.0.7`
2. `docker run --name influxdb -p 8086:8086 influxdb:2.0.7`
3. visit http://{ip}:8086/ to set up user
    1. username: admin
    2. password: 12345678
    3. org: admin
    4. bucket: admin

# config
```

```