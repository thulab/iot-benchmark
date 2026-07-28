package cn.edu.tsinghua.iot.benchmark.mode;

import cn.edu.tsinghua.iot.benchmark.client.DataClient;
import cn.edu.tsinghua.iot.benchmark.client.operation.Operation;
import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsFileLoadRouting;

import java.util.Collections;
import java.util.List;

/** Mode dedicated to measuring external TsFile construction and server-side LOAD separately. */
public class TsFileLoadMode extends BaseMode {
  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  @Override
  protected boolean preCheck() {
    if (config.isIS_DOUBLE_WRITE())
      throw new IllegalArgumentException("tsFileLoadMode does not support double write");
    if (config.getSENSORS().stream()
        .anyMatch(sensor -> sensor.getSensorType() == SensorType.OBJECT)) {
      throw new IllegalArgumentException(
          "tsFileLoadMode does not support OBJECT sensors. Set the OBJECT entry in "
              + "INSERT_DATATYPE_PROPORTION to 0.");
    }
    List<DBConfig> dbs = Collections.singletonList(config.getDbConfig());
    TsFileLoadRouting.validate(config, config.getDbConfig());
    return (!config.isIS_DELETE_DATA() || cleanUpData(dbs))
        && (!config.isCREATE_SCHEMA() || registerSchema());
  }

  @Override
  protected void postCheck() {
    finalMeasure(
        baseModeMeasurement,
        dataClients.stream().map(DataClient::getMeasurement),
        startTime,
        Collections.singletonList(Operation.INGESTION));
  }
}
