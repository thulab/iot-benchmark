package cn.edu.tsinghua.iot.benchmark.iotdb130;

import cn.edu.tsinghua.iot.benchmark.entity.Batch.IBatch;
import cn.edu.tsinghua.iot.benchmark.measurement.Status;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.enums.DBInsertMode;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.GroupByQuery;
import org.slf4j.LoggerFactory;

public class IoTDBSessionBaseTable extends IoTDBSession {

  public IoTDBSessionBaseTable(DBConfig dbConfig) {
    super(dbConfig);
    LOGGER = LoggerFactory.getLogger(IoTDBSessionBaseTable.class);
  }

  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    return null;
  }

  @Override
  public Status insertOneBatch(IBatch batch) {
    DBInsertMode insertMode = dbConfig.getDB_SWITCH().getInsertMode();
    switch (insertMode) {
      case INSERT_USE_SESSION_TABLET:
        return insertOneBatchByTablet(batch);
      case INSERT_USE_SESSION_RECORD:
        return insertOneBatchByRecord(batch);
      default:
        throw new IllegalStateException("Unexpected INSERT_MODE value: " + insertMode);
    }
  }
}
