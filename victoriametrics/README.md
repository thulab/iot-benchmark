VictoriaMetrics
---

# 测试环境
1. 本次测试使用Docker镜像进行正确性验证，拉取的是如下网址的latest的镜像

https://hub.docker.com/r/victoriametrics/victoria-metrics/

2. 环境配置过程
    1. `docker pull victoriametrics/victoria-metrics`
    2. `docker run -it --rm -v /path/to/victoria-metrics-data:/victoria-metrics-data -p 8428:8428 -d --name=victoria victoriametrics/victoria-metrics -retentionPeriod=30 -search.latencyOffset=1s -search.disableCache=true`
    3. 请格外注意环境配置时的retentionPeriod参数的设计，该参数的单位为月，允许插入的时间序列范围为(当前月-retentionPeriod，当前月)
3. 需要对conf文件中的参数进行修改保证可以正确插入