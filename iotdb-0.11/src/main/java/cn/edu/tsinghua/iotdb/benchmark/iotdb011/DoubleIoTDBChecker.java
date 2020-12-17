package cn.edu.tsinghua.iotdb.benchmark.iotdb011;

import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import java.sql.Connection;
import java.sql.Statement;

public class DoubleIoTDBChecker extends DoubleIOTDB {

  @Override
  public Status insertOneBatch(Batch batch) {
    boolean status1 = insertOneConnectionBatch(batch, connection1);
    boolean status2 = insertOneConnectionBatch(batch, connection2);
    if (status1 && status2) {
      return new Status(true);
    } else {
      return new Status(false, 0);
    }
  }

  private boolean insertOneConnectionBatch(Batch batch, Connection connection){
    try (Statement statement = connection.createStatement()) {
      for (Record record : batch.getRecords()) {
        String sql = IoTDB.getInsertOneBatchSql(batch.getDeviceSchema(), record.getTimestamp(),
            record.getRecordDataValue());
        statement.addBatch(sql);
      }
      statement.executeBatch();
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
